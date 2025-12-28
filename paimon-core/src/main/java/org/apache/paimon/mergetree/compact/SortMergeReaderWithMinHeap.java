/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** {@link SortMergeReader} implemented with min-heap. */
// 在 Apache Paimon 中，SortMergeReaderWithMinHeap 是实现 LSM 树 多路归并读取（Multi-way Merge Read） 的核心组件。
// 它使用 最小堆（Min-Heap/Priority Queue） 算法来协调来自多个输入流（不同文件或不同层级）的数据。
// SortMergeReaderWithMinHeap 的主要作用是 有序合并与去重/聚合。
// 在 Paimon 存储中，同一个主键（Primary Key）的数据可能分散在不同的文件或不同的 LSM 层级中。
// 全局排序：从多个 RecordReader 中读取数据，并确保输出结果按主键有序。
// 冲突处理：当发现多个 Reader 产出了相同的主键时，利用 MergeFunctionWrapper（如去重、求和等逻辑）将它们合并为一条记录。
// 流式处理：通过最小堆实时对比多个输入流的“头部”元素，以最小的内存代价实现大规模数据的归并。
public class SortMergeReaderWithMinHeap<T> implements SortMergeReader<T> {
    // 存储待处理的 RecordReader 列表。
    // 如果某个 Reader 的当前数据批次（Batch）读完了，它会被放回这里以便在 readBatch() 时触发加载下一个批次。
    private final List<RecordReader<KeyValue>> nextBatchReaders;
    // 用户主键比较器。决定主键的物理排序顺序。
    private final Comparator<InternalRow> userKeyComparator;
    // 合并逻辑的包装器。当堆顶出现多个相同 Key 的元素时，负责将它们 add 进去并 getResult。
    private final MergeFunctionWrapper<T> mergeFunctionWrapper;
    // 存储每个输入流的当前最领先元素。
    // 排序优先级：首先按 用户主键（升序）；主键相同时按 用户定义的序列比较器；最后按 Paimon 序列号（Sequence Number）。
    private final PriorityQueue<Element> minHeap;
    // 一个临时列表，存放刚从堆中弹出（Poll）的元素。
    // 这些元素拥有相同的主键，处理完后需要更新并重新放入堆。
    private final List<Element> polled;

    public SortMergeReaderWithMinHeap(
            List<RecordReader<KeyValue>> readers,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper) {
        this.nextBatchReaders = new ArrayList<>(readers);
        this.userKeyComparator = userKeyComparator;
        this.mergeFunctionWrapper = mergeFunctionWrapper;

        this.minHeap =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            int result = userKeyComparator.compare(e1.kv.key(), e2.kv.key());
                            if (result != 0) {
                                return result;
                            }
                            if (userDefinedSeqComparator != null) {
                                result =
                                        userDefinedSeqComparator.compare(
                                                e1.kv.value(), e2.kv.value());
                                if (result != 0) {
                                    return result;
                                }
                            }
                            return Long.compare(e1.kv.sequenceNumber(), e2.kv.sequenceNumber());
                        });
        this.polled = new ArrayList<>();
    }
    // 开始或继续一轮批次读取。
    // 它会遍历 nextBatchReaders，从每个 Reader 中预读一个 KeyValue 并封装成 Element 放入最小堆。
    // 如果所有 Reader 都没有数据了，返回 null。
    // 初始化或补充最小堆（Min-Heap），确保每个可用的数据源（Reader）都有一个代表性的元素进入堆中参与排序归并。
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        // nextBatchReaders 这个列表存放了那些“当前没有数据在堆中”的读取器（例如刚启动时所有的读取器，或者之前的批次刚读完的读取器）。
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            while (true) {
                // 调用底层读取器的 readBatch() 方法获取一个数据批次迭代器（RecordIterator）。
                RecordIterator<KeyValue> iterator = reader.readBatch();
                // 如果 readBatch() 返回 null，说明该 reader 已经读取了所有数据文件。
                if (iterator == null) {
                    // no more batches, permanently remove this reader
                    reader.close();
                    break;
                }
                // 从批次中尝试提取第一条记录
                KeyValue kv = iterator.next();
                // 如果 kv 为 null，说明获取到的批次是空的（虽然有 Batch，但里面没数据）
                if (kv == null) {
                    // empty iterator, clean up and try next batch
                    iterator.releaseBatch();
                } else {
                    // found next kv
                    // 将有效记录加入最小堆
                    minHeap.offer(new Element(kv, iterator, reader));
                    break;
                }
            }
        }
        // 清空 nextBatchReaders 列表。
        // 这一步很重要，表示当前所有可用的读取器要么已经有元素进入了堆，要么已经读完并关闭了。
        nextBatchReaders.clear();

        return minHeap.isEmpty() ? null : new SortMergeIterator();
    }

    @Override
    public void close() throws IOException {
        for (RecordReader<KeyValue> reader : nextBatchReaders) {
            reader.close();
        }
        for (Element element : minHeap) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
        for (Element element : polled) {
            element.iterator.releaseBatch();
            element.reader.close();
        }
    }

    /** The iterator iterates on {@link SortMergeReaderWithMinHeap}. */
    // Paimon 进行多路归并时真正执行 “比较、弹出、合并” 逻辑的地方。
    // 管理堆的生命周期：负责将处理完的元素更新并重新放回最小堆。
    // 主键聚合：识别堆顶具有相同主键的所有记录，并将它们交给合并函数。
    // 批次控制：当某个数据源的当前批次读完时，及时停止迭代，通知外层去加载新批次。
    private class SortMergeIterator implements RecordIterator<T> {

        private boolean released = false;

        @Override
        public T next() throws IOException {
            while (true) {
                boolean hasMore = nextImpl();
                if (!hasMore) {
                    return null;
                }
                T result = mergeFunctionWrapper.getResult();
                if (result != null) {
                    return result;
                }
            }
        }
        // 核心归并逻辑
        // 负责从堆中找出下一组相同 Key 的数据
        private boolean nextImpl() throws IOException {
            Preconditions.checkState(
                    !released, "SortMergeIterator#advanceNext is called after release");
            Preconditions.checkState(
                    nextBatchReaders.isEmpty(),
                    "SortMergeIterator#advanceNext is called even if the last call returns null. "
                            + "This is a bug.");

            // add previously polled elements back to priority queue
            // 遍历 polled 列表（存放的是上一组 Key 对应的元素）
            for (Element element : polled) {
                // 获取该数据源的下一条记录
                if (element.update()) {
                    // still kvs left, add back to priority queue
                    // 如果有新记录，重新 offer 进入 minHeap 参与排序。
                    minHeap.offer(element);
                } else {
                    // reach end of batch, clean up
                    // 如果该数据源的当前 Batch 读完了，则调用 releaseBatch() 并将其 Reader 放入 nextBatchReaders，
                    // 此时 nextImpl 会返回 false，触发外层加载新批次
                    element.iterator.releaseBatch();
                    nextBatchReaders.add(element.reader);
                }
            }
            polled.clear();

            // there are readers reaching end of batch, so we end current batch
            if (!nextBatchReaders.isEmpty()) {
                return false;
            }
            // 准备处理这组新的 Key
            mergeFunctionWrapper.reset();
            // 查看堆顶（minHeap.peek()）的元素，将其 Key 作为本次合并的目标主键。
            InternalRow key =
                    Preconditions.checkNotNull(minHeap.peek(), "Min heap is empty. This is a bug.")
                            .kv
                            .key();

            // fetch all elements with the same key
            // note that the same iterator should not produce the same keys, so this code is correct
            // 只要堆顶元素的 Key 与目标 Key 相同
            while (!minHeap.isEmpty()) {
                Element element = minHeap.peek();
                if (userKeyComparator.compare(key, element.kv.key()) != 0) {
                    break;
                }
                // 从堆中弹出（poll）。
                minHeap.poll();
                // 将其 kv 加入合并包装器（add）
                mergeFunctionWrapper.add(element.kv);
                // 将其存入 polled 列表（以便在处理下一个 Key 时能找回这些数据源）
                polled.add(element);
            }
            return true;
        }

        @Override
        public void releaseBatch() {
            released = true;
        }
    }
    // 路归并算法中的基本操作单元，封装了从单个数据源读取到的数据及其上下文。
    // Element 扮演了“流游标（Cursor）”的角色。
    // 在最小堆（PriorityQueue）中，我们并不直接放入 KeyValue，而是放入这个 Element 对象。 它的主要作用是：
    // 持有当前记录：保存当前该数据源中“排在最前面”的那条 KeyValue。
    // 维护来源引用：关联了产生该记录的迭代器（iterator）和读取器（reader），以便在当前记录处理完后，能立刻从同一个来源获取下一条记录。
    // 状态更新：提供自我更新的能力，使游标能够向后移动。
    private static class Element {
        // 存储当前该路径（流）中待处理的最新记录。
        private KeyValue kv;
        // 当前数据批次（Batch）的迭代器。
        private final RecordIterator<KeyValue> iterator;
        // 底层的读取器对象（对应一个物理文件或内存段）。
        private final RecordReader<KeyValue> reader;

        private Element(
                KeyValue kv, RecordIterator<KeyValue> iterator, RecordReader<KeyValue> reader) {
            this.kv = kv;
            this.iterator = iterator;
            this.reader = reader;
        }

        // IMPORTANT: Must not call this for elements still in priority queue!
        // 尝试从当前的迭代器中“步进”到下一条记录。
        // 由于最小堆的结构是基于 kv 进行排序的，
        // 如果在 Element 还在堆中时修改了它的 kv 值，会破坏堆的有序性（Violation of Heap Invariant），导致归并结果乱序。
        // 因此，必须先将元素 poll 出堆，更新后，再重新 offer 进堆。
        private boolean update() throws IOException {
            KeyValue nextKv = iterator.next();
            if (nextKv == null) {
                return false;
            }
            kv = nextKv;
            return true;
        }
    }
}

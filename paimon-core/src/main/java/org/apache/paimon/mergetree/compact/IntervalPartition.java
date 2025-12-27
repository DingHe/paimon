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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/** Algorithm to partition several data files into the minimum number of {@link SortedRun}s. */
// 在 Apache Paimon 的 Merge Tree（合并树）架构中，
// IntervalPartition 是一个极其核心的算法类。
// 它决定了如何将杂乱的、可能有主键重叠的数据文件，科学地组织成“互不重叠”或“最小冲突”的逻辑组。
// IntervalPartition 的作用可以概括为：将一组数据文件在主键维度上进行“解耦”和“分组”。
// 在主键表中，如果文件 A 包含主键 1~10，文件 B 包含主键 5~15，它们就存在区间重叠。读取时，必须将它们放在一起合并，否则同一个主键的数据就会出现多份，违反主键约束。
// 该类通过两层算法实现目标：
// Section 划分（外层）：将文件划分为若干个 Section。不同 Section 之间的主键区间完全不重叠。这意味着不同 Section 可以完全并行处理，互不干扰。
// SortedRun 划分（内层）：在同一个 Section 内部，将文件划分为最少数量的 SortedRun（有序链）。每个 SortedRun 内部的文件是按主键顺序排列且互不重叠的。

public class IntervalPartition {
    // 待处理的数据文件元数据列表。
    // 在构造函数中，这些文件会先按 minKey 升序、再按 maxKey 升序进行排序。
    private final List<DataFileMeta> files;
    // 主键比较器。这是算法的灵魂，用于判断两个主键值的先后顺序，从而确定文件区间是否重叠。
    private final Comparator<InternalRow> keyComparator;
    // 初始化并对输入文件进行预排序
    public IntervalPartition(List<DataFileMeta> inputFiles, Comparator<InternalRow> keyComparator) {
        this.files = new ArrayList<>(inputFiles);
        this.files.sort(
                (o1, o2) -> {
                    int leftResult = keyComparator.compare(o1.minKey(), o2.minKey());
                    return leftResult == 0
                            ? keyComparator.compare(o1.maxKey(), o2.maxKey())
                            : leftResult;
                });
        this.keyComparator = keyComparator;
    }

    /**
     * Returns a two-dimensional list of {@link SortedRun}s.
     *
     * <p>The elements of the outer list are sections. Key intervals between sections do not
     * overlap. This extra layer is to minimize the number of {@link SortedRun}s dealt at the same
     * time.
     *
     * <p>The elements of the inner list are {@link SortedRun}s within a section.
     *
     * <p>Users are expected to use the results by this way:
     *
     * <pre>{@code
     * for (List<SortedRun> section : algorithm.partition()) {
     *     // do some merge sorting within section
     * }
     * }</pre>
     */
    // Paimon 主键表读取逻辑中最重要的“预处理”步骤
    // 标是将一大堆杂乱的数据文件，按照**主键范围（Interval）**切分成一个个互相独立的“孤岛”（Sections）
    public List<List<SortedRun>> partition() {
        // 外层 List 代表不同的 Section，内层 List 代表该 Section 内部的 SortedRun（有序链）。
        List<List<SortedRun>> result = new ArrayList<>();
        // 创建一个临时列表，用于存放当前正在构建的 Section 里的文件。
        List<DataFileMeta> section = new ArrayList<>();
        // 定义当前 Section 的右边界（最大主键值）
        BinaryRow bound = null;

        for (DataFileMeta meta : files) {
            // 判断是否需要开启一个新的 Section
            // 如果当前待处理文件的“最小主键”比目前 Section 的“最大右边界”还要大，说明这个文件和当前组里的任何文件都不可能有交集
            if (!section.isEmpty() && keyComparator.compare(meta.minKey(), bound) > 0) {
                // larger than current right bound, conclude current section and create a new one
                // 当发现新文件不重叠时，结算当前 Section
                result.add(partition(section));
                section.clear();
                bound = null;
            }
            section.add(meta);
            // 更新当前 Section 的右边界
            if (bound == null || keyComparator.compare(meta.maxKey(), bound) > 0) {
                // update right bound
                bound = meta.maxKey();
            }
        }
        if (!section.isEmpty()) {
            // conclude last section
            result.add(partition(section));
        }

        return result;
    }
    // 目标非常明确：在已经确定有 Key 重叠的一个 Section 内部，将这些文件分配到尽可能少数量的 SortedRun 中。
    private List<SortedRun> partition(List<DataFileMeta> metas) {
        // 创建一个优先队列，其存储的元素是一个个文件列表（即未来的 SortedRun）
        // 按照每个列表里**最后一个文件的最大 Key（maxKey）**进行升序排列。
        // 队列的头部始终是那个“结束最早（右边界最小）”的有序链。
        // 这为后续判断新文件能否“接”在现有链后面提供了最高效的查找方式。
        PriorityQueue<List<DataFileMeta>> queue =
                new PriorityQueue<>(
                        (o1, o2) ->
                                // sort by max key of the last data file
                                keyComparator.compare(
                                        o1.get(o1.size() - 1).maxKey(),
                                        o2.get(o2.size() - 1).maxKey()));
        // create the initial partition
        // 将 Section 内的第一个文件放入一个新的列表中，并存入队列。
        List<DataFileMeta> firstRun = new ArrayList<>();
        firstRun.add(metas.get(0));
        queue.add(firstRun);

        for (int i = 1; i < metas.size(); i++) {
            DataFileMeta meta = metas.get(i);
            // any file list whose max key < meta.minKey() is sufficient,
            // for convenience we pick the smallest
            List<DataFileMeta> top = queue.poll();
            // 如果当前文件的 minKey 大于该链最后一个文件的 maxKey，说明它们不重叠，且当前文件可以合法的接在后面并保持有序。
            if (keyComparator.compare(meta.minKey(), top.get(top.size() - 1).maxKey()) > 0) {
                // append current file to an existing partition
                top.add(meta);
            } else {
                // create a new partition
                // 如果当前文件与结束最早的链都重叠了，那它肯定与队列中其他的链也都重叠（因为其他的链结束更晚）。此时，必须为当前文件开启一个新的 SortedRun。
                List<DataFileMeta> newRun = new ArrayList<>();
                newRun.add(meta);
                queue.add(newRun);
            }
            // 将之前弹出的 top 链重新放回队列。
            // 如果刚才执行了拼接，它的 maxKey 更新了，优先队列会自动调整它的位置。
            queue.add(top);
        }

        // order between partitions does not matter
        // 将队列中所有的 List<DataFileMeta> 包装成标准的 SortedRun 对象并返回。
        // 这里生成的 SortedRun 数量就是读取该 Section 时需要进行的最小归并路数（Merge Way）。
        return queue.stream().map(SortedRun::fromSorted).collect(Collectors.toList());
    }
}

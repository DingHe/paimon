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

import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;

/**
 * This reader is to read a list of {@link RecordReader}, which is already sorted by key and
 * sequence number, and perform a sort merge algorithm. {@link KeyValue}s with the same key will
 * also be combined during sort merging.
 *
 * <p>NOTE: {@link KeyValue}s from the same {@link RecordReader} must not contain the same key.
 */
// 在 Apache Paimon 的核心存储引擎中，SortMergeReader 是实现 Merge-on-Read (读时合并) 机制的核心组件。
// 它位于 LSM-Tree 结构之上，负责将来自不同层级、不同文件的有序数据流合并为一个统一、正确的结果流。
// 在 Paimon 的主键表中，由于数据是不断 Append 的，同一个主键（Key）可能存在于多个不同的数据文件中（例如：一个在 Base 文件里，另一个在增量 Delta 文件里）。 该类的核心职责包括：
// 多路归并：同时读取多个已经按 Key 排序的 RecordReader。
// 版本去重/合并：当发现多个 Reader 输出相同的 Key 时，根据 Sequence Number（序列号）判断先后顺序。
// 函数转换：利用 MergeFunction 处理相同 Key 的数据（如保留最新的一行 Deduplicate，或是进行部分列更新 PartialUpdate）。
public interface SortMergeReader<T> extends RecordReader<T> {
    // 根据用户的配置和硬件特性，选择最适合的归并引擎。
    static <T> SortMergeReader<T> createSortMergeReader(
            List<RecordReader<KeyValue>> readers, // 待合并的输入流列表。注意项：正如类注释所言，每一个单独的 Reader 内部必须是按 Key 有序的，且同一个 Reader 内部不能有重复的 Key。
            Comparator<InternalRow> userKeyComparator, // 用户主键比较器。用于判断不同数据行是否属于同一个 Key。
            @Nullable FieldsComparator userDefinedSeqComparator, // 用户定义的序列号比较器。当 Key 相同时，系统依靠它来决定哪条数据更“新”或者优先级更高。
            MergeFunctionWrapper<T> mergeFunctionWrapper, // 合并逻辑的包装器。它定义了当 Key 冲突时，是采取“去重（Deduplicate）”、“聚合（Aggregation）”还是“覆盖（Overwrite）”操作。
            SortEngine sortEngine) { // 排序引擎类型。决定底层使用哪种数据结构进行多路找最小值。
        switch (sortEngine) {
            case MIN_HEAP:
                return new SortMergeReaderWithMinHeap<>(
                        readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
            case LOSER_TREE:
                return new SortMergeReaderWithLoserTree<>(
                        readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
            default:
                throw new UnsupportedOperationException("Unsupported sort engine: " + sortEngine);
        }
    }
}

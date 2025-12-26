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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/** Generate splits from {@link DataFileMeta}s. */
// 定义了如何将底层的 Data Files（数据文件） 组织成可以并行处理的 Splits（分片）。这是连接存储层和计算层（如 Flink/Spark）的关键桥梁。
// plitGenerator 的主要作用是 “逻辑分组”。
// 在 Paimon 的 LSM 树结构中，一个 Bucket 内可能存在多个 Level 的数据文件。为了让计算引擎高效读取，不能简单地把每个文件当成一个任务，也不能把所有文件堆在一起。
// 并行化基础：它将 DataFileMeta（文件元数据）列表划分为多个 SplitGroup。每个 SplitGroup 后续会封装成一个 Split，分发给不同的并行任务（Task/Executor）执行。
// 读取路径优化：它会根据文件的重叠情况（Overlapping）决定该分片是否可以“原始转换”（Raw Convertible）。如果一个分片内的文件互不重叠，计算引擎可以直接读取原始文件，无需在内存中进行复杂的归并排序（Merge Sort），从而极大提升性能。
// 适配不同模式：针对批处理（Batch）和流处理（Streaming）提供不同的切分策略。
public interface SplitGenerator {
    // 返回 true 表示该生成器生成的所有分片都保证是 rawConvertible 的。
    // 应用场景：例如在 Append-only（仅追加）类型的表中，文件永远不会重叠，因此总是可以返回 true。
    boolean alwaysRawConvertible();
    // 针对批处理模式进行分片。
    // 逻辑说明：在批模式下，通常会考虑更多的全局优化，比如尽可能将小文件合并到一个分片中，或者根据文件的 Key 范围进行更精细的划分，以平衡并行度。
    List<SplitGroup> splitForBatch(List<DataFileMeta> files);

    // 针对流处理模式进行分片。
    // 流模式通常处理的是增量数据（Delta）。该方法会根据流式消费的特点（如顺序性、增量快照）来组织文件，确保数据能够正确地流转。
    List<SplitGroup> splitForStreaming(List<DataFileMeta> files);

    /** Split group. */
    // 分片的逻辑载体，包含了文件列表以及读取属性。
    class SplitGroup {
        // 该分片包含的所有物理数据文件的元数据。计算引擎会根据这些元数据去文件系统读取实际数据。
        public final List<DataFileMeta> files;
        // true: 表示这些文件之间没有 Key 重叠。
        // 读取时可以直接由底层的 Reader 读取并返回，无需经过 Paimon 的 MergeTree 聚合逻辑。
        public final boolean rawConvertible;

        private SplitGroup(List<DataFileMeta> files, boolean rawConvertible) {
            this.files = files;
            this.rawConvertible = rawConvertible;
        }

        public static SplitGroup rawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, true);
        }

        public static SplitGroup nonRawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, false);
        }
    }
}

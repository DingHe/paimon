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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A scan of {@link Table} to generate {@link Split} splits.
 *
 * @since 0.4.0
 */
// TableScan 是数据读取链路的核心入口接口。
// 它的主要任务是“制定计划”——即将存储在文件系统上的海量数据，根据查询条件切分成一个个可并行处理的任务单元（Splits）。
// TableScan 处于查询引擎（如 Flink, Spark, Trino）与 Paimon 底层存储文件之间的中间层。其核心作用可以概括为：
// 任务拆分 (Splitting)：将一张逻辑表的数据拆分为多个 Split。每个 Split 通常对应一组数据文件，可以分发给不同的计算节点并行读取。
// 元数据过滤 (Pruning)：在扫描过程中，利用 Snapshot、Manifest 和统计信息（Min/Max）进行分区裁剪和文件裁剪，确保只读取必要的数据，减少 I/O。
// 流批统一：无论是读取历史存量数据的“批扫描”，还是读取增量变更的“流扫描”，都通过该接口的不同实现类来统一调度。

@Public
public interface TableScan {

    /** Set {@link MetricRegistry} to table scan. */
    // 为扫描器注入指标注册表
    // Paimon 在扫描时会收集一系列性能指标（Metrics），例如“扫描耗时”、“处理的清单文件数量”、“过滤掉的文件数量”等。
    TableScan withMetricRegistry(MetricRegistry registry);

    /** Plan splits, throws {@link EndOfScanException} if the scan is ended. */
    // 执行实际的扫描规划，生成分片计划。
    // 这是 TableScan 最重要的方法。调用时，它会触发元数据的检索流程：从指定的 Snapshot 开始，遍历 Manifest List 和 Manifest File。
    // 它会应用所有的下推过滤条件（Filters），决定哪些数据文件需要被读取。
    Plan plan();

    /** List partitions. */
    // 获取表中所有分区的详细元数据条目。
    // 每个 PartitionEntry 不仅包含分区的 BinaryRow 标识，通常还包含了该分区的统计信息（如记录数、文件数等）。这在进行分区管理或高级查询优化时非常有用。
    default List<BinaryRow> listPartitions() {
        return listPartitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toList());
    }

    // 获取表中所有分区的列表（仅包含分区值）
    List<PartitionEntry> listPartitionEntries();

    /**
     * Plan of scan.
     *
     * @since 0.4.0
     */
    @Public
    interface Plan {
        List<Split> splits();
    }
}

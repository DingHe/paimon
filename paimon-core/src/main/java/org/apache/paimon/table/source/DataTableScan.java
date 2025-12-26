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

/** Table scan for data table. */
// DataTableScan 的核心作用是实现 数据分片（Sharding）与并行执行规划。
// 在分布式计算环境（如 Flink 或 Spark）中，一张表的数据往往非常庞大。DataTableScan 不仅继承了父接口的过滤和裁剪功能，还引入了**分片（Shard）**的概念：
// 并行度感知：它允许扫描器感知外部计算引擎的并行度
// 负载均衡：通过分片机制，它能确保每个并行子任务（Subtask）只分配到属于自己处理的那部分文件，从而实现数据的均匀分布和高效读取。

// 为了清晰理解其位置，可以参考以下层级关系：
//TableScan (最外层接口): 提供基础的 plan() 和 listPartitions() 功能。
//InnerTableScan (内部接口): 增加了大量 withFilter、withBucket 等谓词下推方法，用于减少扫描的文件量。
//DataTableScan (数据表专项接口): 在过滤的基础上，增加了 withShard，用于计算节点间的任务切分。

public interface DataTableScan extends InnerTableScan {

    /** Specify the shard to be read, and allocate sharded files to read records. */
    // 指定当前子任务要读取的分片，并据此分配对应的文件。
    // indexOfThisSubtask (当前子任务索引)：代表当前计算节点的 ID（从 0 开始）。例如，Flink 算子的第 3 个并发实例。
    // numberOfParallelSubtasks (总并行度)：代表当前作业的总并发数。
    DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);
}

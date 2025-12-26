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

import org.apache.paimon.table.source.snapshot.StartingContext;

import javax.annotation.Nullable;

/** Streaming {@link InnerTableScan} with {@link StreamTableScan}. */
// 同时继承了 DataTableScan 和 StreamTableScan，是 Data Table（数据表）进行流式增量读取 的核心入口。
// StreamDataTableScan 的核心作用是实现 “具有分片能力的流式增量扫描”。
// 流式与分片的结合：它不仅具备流式读取（记录进度、支持恢复）的能力，还具备数据表的分片（Sharding）能力，使得多个并行实例可以同时流式消费同一张表的不同部分。
// 启动策略管理：它负责定义流式读取从哪里开始（是从最新快照、最早快照，还是指定的 ID 开始）。
// 精确的状态恢复：它增强了恢复机制，不仅能恢复快照 ID，还能明确恢复时的扫描模式（是扫描该快照的全量数据还是仅增量数据）。
public interface StreamDataTableScan extends DataTableScan, StreamTableScan {
    // 获取流式读取的起始上下文信息。
    StartingContext startingContext();

    /** Restore from checkpoint next snapshot id with scan kind. */
    void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot);
}

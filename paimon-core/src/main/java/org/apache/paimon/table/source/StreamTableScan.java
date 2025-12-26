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
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

/**
 * {@link TableScan} for streaming, supports {@link #checkpoint} and {@link #restore}.
 *
 * <p>NOTE: {@link #checkpoint} will return the next snapshot id.
 *
 * @since 0.4.0
 */

// StreamTableScan 是专门为流式数据读取设计的扫描接口。
// 它继承了 TableScan 和 Restorable<Long>，是实现增量消费数据（Incremental Consumption）的核心组件。
// StreamTableScan 的核心作用是作为 Paimon 表与流计算引擎（如 Flink）之间的桥梁，支持“像读消息队列一样读取湖仓表”：
// 状态化增量扫描：与批扫描（Batch Scan）一次性规划所有分片不同，流扫描会不断寻找新产生的 Snapshot 或 Changelog。
// 精确一次（Exactly-once）保证：通过实现 Restorable<Long> 接口，它能够记录当前读取到的快照 ID，并支持在任务失败后从该 ID 恢复，确保数据不重不漏。
// 水位线（Watermark）传递：支持将 Paimon 快照中携带的逻辑水位线传递给计算引擎，实现流式任务的事件时间驱动。

@Public
public interface StreamTableScan extends TableScan, Restorable<Long> {

    /** Current watermark for consumed snapshot. */
    // 获取当前已消费快照的关联水位线。
    // 在 Paimon 的写入端，通常会将上游系统的 Watermark 记录在 Snapshot 元数据中
    // 当 StreamTableScan 消费到某个快照时，通过此方法可以获取该快照对应的 watermark 值。
    // 计算引擎（如 Flink）利用此值来对齐整个流作业的事件时间。
    @Nullable
    Long watermark();

    /** Restore from checkpoint next snapshot id. */
    // 根据保存的快照 ID 恢复扫描进度。
    // 当 Flink 任务从 Checkpoint/Savepoint 重启时，Source 会调用此方法，将状态后端存储的 ID 重新注入扫描器，让扫描器跳过已经消费过的快照。
    @Override
    void restore(@Nullable Long nextSnapshotId);

    /** Checkpoint to return next snapshot id. */
    // 提取当前的读取进度，用于保存到 Checkpoint。
    @Nullable
    @Override
    Long checkpoint();

    /** Notifies the checkpoint complete with next snapshot id. */
    // 通知扫描器 Checkpoint 已经成功完成。
    void notifyCheckpointComplete(@Nullable Long nextSnapshot);
}

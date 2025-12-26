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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;

import javax.annotation.Nullable;

import java.util.List;

/** Helper class for the first planning of {@link TableScan}. */
// 主要作用是确定读取任务的起点并执行首次数据检索。
// 由于 Paimon 支持多种启动模式（如从最早、最新、特定时间戳或特定快照启动），系统需要一个统一的接口来屏蔽这些复杂逻辑。StartingScanner 的存在价值在于：
// 策略实现：各种子类（如 FullStartingScanner, ContinuousLatestStartingScanner 等）实现了不同的“寻找起点”的算法。
// 状态初始化：它不仅告诉你从哪个快照开始，还会负责执行该快照的初始扫描（如果需要），并把扫描结果（Splits）打包返回。
// 流批统一的起点：无论是批处理还是流处理，都通过它来获取任务的初始状态。
public interface StartingScanner {
    // 获取本次扫描计算出的初始上下文。
    StartingContext startingContext();
    // 执行实际的首次扫描操作。
    // 接收一个 SnapshotReader，根据实现的策略寻找目标快照，并决定是读取该快照的全部数据、增量数据，还是仅仅记录一个位置。
    Result scan(SnapshotReader snapshotReader);
    // 专门用于获取起始状态下涉及的分区信息。
    // 在某些场景下（如分区裁剪或元数据查询），调用者只需要知道有哪些分区，而不需要具体的物理分片（Splits）。
    List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader);

    /** Scan result of {@link #scan}. */
    interface Result {}

    /** Currently, there is no snapshot, need to wait for the snapshot to be generated. */
    // 表示当前表中没有任何快照。
    class NoSnapshot implements Result {}

    static ScannedResult fromPlan(SnapshotReader.Plan plan) {
        return new ScannedResult(plan);
    }

    /** Result with scanned snapshot. Next snapshot should be the current snapshot plus 1. */
    // 表示已经成功扫描到数据，且包含具体的物理分片。
    class ScannedResult implements Result {

        private final SnapshotReader.Plan plan;

        public ScannedResult(SnapshotReader.Plan plan) {
            this.plan = plan;
        }

        public long currentSnapshotId() {
            return plan.snapshotId();
        }

        @Nullable
        public Long currentWatermark() {
            return plan.watermark();
        }

        public List<DataSplit> splits() {
            return (List) plan.splits();
        }

        public SnapshotReader.Plan plan() {
            return plan;
        }
    }

    /**
     * Return the next snapshot for followup scanning. The current snapshot is not scanned (even
     * doesn't exist), so there are no splits.
     */
    // 表示当前快照不读取数据，直接跳转到下一个。
    class NextSnapshot implements Result {
        // 记录了下一个要监控的快照 ID。
        private final long nextSnapshotId;

        public NextSnapshot(long nextSnapshotId) {
            this.nextSnapshotId = nextSnapshotId;
        }

        public long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}

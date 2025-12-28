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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_SNAPSHOT} startup mode of a
 * streaming read.
 */
// 在 Apache Paimon 中，ContinuousFromSnapshotStartingScanner 是专门用于流式读取（Streaming Read）场景下的启动扫描器。
// 它对应于配置项 scan.startup.mode: from-snapshot。
// 确定流式作业启动时的“切入点”快照 ID。
// 与批处理扫描器（直接返回文件列表）不同，它是流式读取的“导航员”。它不直接读取数据文件，而是返回一个 NextSnapshot 信号。
// 这个信号告诉 Paimon 的枚举器（Enumerator）：“请从指定的这个快照 ID 开始，监听并捕获后续产生的增量数据。”
// 它解决了两个关键问题：
// 用户指定的快照 ID 是否还存在（是否已被清理）？
// 如果用户指定的 ID 太旧了，应该从哪里合法地开始？
public class ContinuousFromSnapshotStartingScanner extends AbstractStartingScanner {
    // 标识 Changelog 是否解耦（分离）存储
    // 在 Paimon 中，如果配置了“长生命周期 Changelog”（Long-lived Changelog），Changelog 的保存时间可以独立于 Snapshot。
    // 如果该值为 true，意味着即使 Snapshot 被删除了，只要对应的 Changelog 还在，作业依然可以从那个位置启动。
    private final boolean changelogDecoupled;
    // 专门负责管理 Changelog 文件的组件。
    private final ChangelogManager changelogManager;

    public ContinuousFromSnapshotStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long snapshotId,
            boolean changelogDecoupled) {
        super(snapshotManager);
        this.changelogManager = changelogManager;
        this.startingSnapshotId = snapshotId;
        this.changelogDecoupled = changelogDecoupled;
    }
    // 计算流式启动的起始快照结果。
    @Override
    public Result scan(SnapshotReader snapshotReader) {
        // 获取系统中当前能找到的最老的数据点。
        Long earliestId = getEarliestId();
        // 如果系统中完全没有任何快照（earliestId == null），返回 NoSnapshot，流式作业将进入等待状态，直到第一个快照产生。
        if (earliestId == null) {
            return new NoSnapshot();
        }
        // We should return the specified snapshot as next snapshot to indicate to scan delta data
        // from it. If the snapshotId < earliestSnapshotId, start from the earliest.
        // 用户指定的 startingSnapshotId 可能已经被后台清理（TTL 过期）
        // 代码使用 Math.max(startingSnapshotId, earliestId) 进行兜底
        return new NextSnapshot(Math.max(startingSnapshotId, earliestId));
    }
    // 计算系统中“理论上最早”可读的 ID。
    private Long getEarliestId() {
        Long earliestId;
        if (changelogDecoupled) {
            Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
            earliestId =
                    earliestChangelogId == null
                            ? snapshotManager.earliestSnapshotId()
                            : earliestChangelogId;
        } else {
            earliestId = snapshotManager.earliestSnapshotId();
        }
        return earliestId;
    }
}

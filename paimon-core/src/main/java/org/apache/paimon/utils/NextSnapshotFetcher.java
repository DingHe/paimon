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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.OutOfRangeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Fetcher for getting the next snapshot by snapshot id. */
// 主要负责在流式作业运行过程中，按照快照 ID 的顺序，安全、准确地获取下一个待处理的快照对象。
// 在流处理中，系统会持有一个 nextSnapshotId 指针，NextSnapshotFetcher 的作用就是去底层存储（文件系统）中寻找这个 ID 对应的元数据。
// 多源检索：不仅查找标准的 Snapshot，在启用 Changelog 解耦时还会查找长生命周期的 Changelog。
// 范围校验（Safety Guard）：防止流式作业读取已过期（被清理）的快照，或者在表被重建后读取到非法的快照 ID。

public class NextSnapshotFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(NextSnapshotFetcher.class);
    // 围检查间隔。
    // 为了避免每次没搜到快照都去列表磁盘（检查最早/最晚 ID），设定每尝试 16 次才执行一次深度的 rangeCheck。
    public static final int RANGE_CHECK_INTERVAL = 16;
    // 负责管理标准快照（Snapshot）的读写和存在性检查。
    private final SnapshotManager snapshotManager;
    // 负责管理独立于快照的变更日志（Changelog）元数据。
    private final ChangelogManager changelogManager;
    // 标识是否开启了 Changelog 解耦。如果开启，即使 Snapshot 文件因为过期被删除了，
    // 只要对应的 Changelog 文件还在，流式任务就可以继续运行。
    private final boolean changelogDecoupled;
    // 内部计数器。记录自上次成功获取快照以来，连续尝试获取失败的次数。
    private int rangeCheckCnt = 0;

    public NextSnapshotFetcher(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean changelogDecoupled) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.changelogDecoupled = changelogDecoupled;
    }
    // 核心入口方法
    @Nullable
    public Snapshot getNextSnapshot(long nextSnapshotId) {
        // 如果存在，重置 rangeCheckCnt 为 0，并直接返回该快照。
        if (snapshotManager.snapshotExists(nextSnapshotId)) {
            rangeCheckCnt = 0;
            return snapshotManager.snapshot(nextSnapshotId);
        }
        // 如果上述不存在，且 changelogDecoupled 为 true，则去 changelogManager 中查找。
        if (changelogDecoupled && changelogManager.longLivedChangelogExists(nextSnapshotId)) {
            return changelogManager.changelog(nextSnapshotId);
        }

        rangeCheckCnt++;
        if (rangeCheckCnt % RANGE_CHECK_INTERVAL == 0) {
            rangeCheck(nextSnapshotId);
        }

        return null;
    }

    private void rangeCheck(long nextSnapshotId) {
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        Long latestSnapshotId = snapshotManager.latestSnapshotIdFromFileSystem();

        // No snapshot now
        if (earliestSnapshotId == null || earliestSnapshotId <= nextSnapshotId) {
            if ((earliestSnapshotId == null && nextSnapshotId > 1)
                    || (latestSnapshotId != null && nextSnapshotId > latestSnapshotId + 1)) {
                throw new OutOfRangeException(
                        String.format(
                                "The next expected snapshot is too big! Most possible cause might be the table had been recreated."
                                        + "The next snapshot id is %d, while the latest snapshot id is %s",
                                nextSnapshotId, latestSnapshotId));
            }

            LOG.debug(
                    "Next snapshot id {} does not exist, wait for the snapshot generation.",
                    nextSnapshotId);
        } else {
            if (!changelogDecoupled) {
                throw new OutOfRangeException(
                        String.format(
                                "The snapshot with id %d has expired. You can: "
                                        + "1. increase the snapshot or changelog expiration time. "
                                        + "2. use consumer-id to ensure that unconsumed snapshots will not be expired.",
                                nextSnapshotId));
            }
        }
    }
}

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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;

/** {@link FollowUpScanner} for read all file changes. */
// 主要用于需要监控所有物理文件变更的场景
// 不加过滤地扫描每一个新快照，并提取该快照中所有的文件变动（新增和删除）。
// 与其他扫描器不同，它不关心提交的类型（无论是 APPEND、COMPACT 还是 OVERWRITE），只要有新快照产生，它就会触发扫描。
// 它通常与 StreamScanMode.FILE_MONITOR 配合使用，用于那些需要感知表底层存储结构变化的特殊任务。
public class AllDeltaFollowUpScanner implements FollowUpScanner {
    // 决定是否扫描给定的快照。
    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        return true;
    }

    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        // 会返回一个差异计划，不仅包含新增的文件，还包含被删除（DELETE）的文件
        return snapshotReader.withMode(ScanMode.DELTA).withSnapshot(snapshot).readChanges();
    }
}

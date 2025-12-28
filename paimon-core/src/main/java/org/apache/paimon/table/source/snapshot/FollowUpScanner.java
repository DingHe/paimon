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
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader.Plan;

/** Helper class for the follow-up planning of {@link StreamTableScan}. */
// 定义了在流式作业启动（Starting）之后，如何持续追踪和处理后续（Follow-up）新产生快照的行为规范。
// 在流式读取过程中，作业会不断发现新的快照（Snapshot）。但是，并不是每一个快照都需要以同样的方式处理。例如：
//有些快照是由于“合并文件（Compact）”产生的，并不包含新数据。
//有些快照是由于“覆盖写（Overwrite）”产生的。
//有些快照包含了物理的 Changelog 文件。
//FollowUpScanner 的作用就是针对每一个新发现的快照，决定“是否需要处理”以及“如何提取增量数据”。它为 DataTableStreamScan 提供了后续处理的插件化实现。


public interface FollowUpScanner {
    // 判断当前快照是否应该被扫描。
    // true 表示需要扫描，false 表示跳过
    boolean shouldScanSnapshot(Snapshot snapshot);
    // 执行真正的增量扫描，生成读取计划（Plan）。
    Plan scan(Snapshot snapshot, SnapshotReader snapshotReader);
    // 专门处理由 INSERT OVERWRITE 操作产生的快照变更。
    default Plan getOverwriteChangesPlan(
            Snapshot snapshot, SnapshotReader snapshotReader, boolean isAppend) {
        if (isAppend) {
            return snapshotReader.withSnapshot(snapshot).withMode(ScanMode.DELTA).read();
        } else {
            return snapshotReader.withSnapshot(snapshot).readChanges();
        }
    }
}

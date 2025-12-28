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
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link FollowUpScanner} for {@link CoreOptions.ChangelogProducer#NONE} changelog producer. */
// 门用于处理没有配置物理 Changelog 产生的表（即 changelog-producer = none）。
// 通过读取快照中新增的数据文件（Delta Files）来实现增量数据监听。
// 当一个 Paimon 表没有配置专门的 Changelog 生成器时，流式作业如果想要获取增量，最直接的方法就是查看每次提交（Commit）新增加了哪些文件。该类负责：
// 识别有效提交：过滤掉那些不产生新数据的元数据操作（如压缩、清理）。
// 提取增量文件：指挥读取器仅加载快照中标记为 ADD 的数据文件。

public class DeltaFollowUpScanner implements FollowUpScanner {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFollowUpScanner.class);
    // 决定当前的快照是否包含需要处理的新数据。
    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        // 在 Paimon 中，只有 APPEND 类型的提交才代表有真正的新数据流入。
        if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
            return true;
        }

        LOG.debug(
                "Next snapshot id {} is not APPEND, but is {}, check next one.",
                snapshot.id(),
                snapshot.commitKind());
        return false;
    }

    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        // 它告诉 SnapshotReader：“只扫描这个快照里新增加的那部分清单项（Manifest Entry）”。
        return snapshotReader.withMode(ScanMode.DELTA).withSnapshot(snapshot).read();
    }
}

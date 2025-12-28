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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link FollowUpScanner} for tables with changelog producer. */
// 专门用于那些配置了 Changelog Producer（如 input、lookup 或 full-compaction）的表。
// 在流式读取过程中，强制要求读取物理的 Changelog 文件，而不是通过对比数据文件来生成增量。
// 当 Paimon 表配置了 changelog-producer 时，每次 Commit 都会产生专门的 .changelog 文件，这些文件精确记录了数据的 INSERT、UPDATE_BEFORE、UPDATE_AFTER 和 DELETE。ChangelogFollowUpScanner 的职责就是：
// 过滤：确保只处理那些真正包含 Changelog 元数据的快照。
// 导流：指挥 SnapshotReader 进入 CHANGELOG 扫描模式。
public class ChangelogFollowUpScanner implements FollowUpScanner {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogFollowUpScanner.class);
    // 决定当前这个新快照是否值得被扫描。
    // 在 Paimon 架构中，快照元数据里如果 changelogManifestList 不为空，说明该快照在提交时物理地生成了 Changelog 文件。
    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        if (snapshot.changelogManifestList() != null) {
            return true;
        }

        LOG.debug("Next snapshot id {} has no changelog, check next one.", snapshot.id());
        return false;
    }
    // 执行真正的物理扫描逻辑。
    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        // 不要去读 data 目录下的文件，去读 changelog 目录下的文件！”
        return snapshotReader.withMode(ScanMode.CHANGELOG).withSnapshot(snapshot).read();
    }
}

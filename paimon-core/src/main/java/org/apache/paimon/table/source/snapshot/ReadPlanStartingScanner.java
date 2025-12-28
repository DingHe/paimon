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
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** An {@link AbstractStartingScanner} to return plan. */
// 专门用于那些需要“先配置读取器，再生成物理执行计划”的启动场景。
// 在 Paimon 中，有些启动模式（如 Full 全量模式、FromSnapshot 指定快照模式）需要扫描特定的快照并产生具体的物理分片（Splits）。该类将这一过程抽象为两个步骤：
// 配置（Configure）：确定要读哪个快照，并设置好过滤条件。
// 执行（Read）：调用读取器生成 Plan（包含文件列表）
public abstract class ReadPlanStartingScanner extends AbstractStartingScanner {

    ReadPlanStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
    }

    @Nullable
    protected abstract SnapshotReader configure(SnapshotReader snapshotReader);

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return new NoSnapshot();
        }
        return StartingScanner.fromPlan(configured.read());
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return Collections.emptyList();
        }
        return configured.partitionEntries();
    }
}

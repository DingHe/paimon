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
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The abstract class for StartingScanner. */
// 位于数据读取的最前端，决定了任务启动时该从哪个位置（Snapshot）开始读，以及以什么模式读。
// 当一个 Flink 或 Spark 任务启动并读取 Paimon 表时，它面临的首要问题是：“我是该从头开始读所有历史数据（全量），还是只读从现在开始产生的新数据（增量），或者是从某个特定的时间点/快照点开始读？”
// 定义了获取启动上下文（Context）和扫描分区（Partitions）的标准行为，具体的扫描逻辑（例如按时间、按 ID、读最新等）由其子类（如 FullStartingScanner、LatestStartingScanner 等）实现。


public abstract class AbstractStartingScanner implements StartingScanner {
    // 快照管理器。
    // Paimon 访问元数据的核心组件。通过它，Scanner 可以查找当前最新的快照 ID、查找某个时间点对应的快照、或者判断某个快照是否已经被清理。
    protected final SnapshotManager snapshotManager;
    // 记录最终确定的启动快照 ID。
    protected Long startingSnapshotId = null;

    AbstractStartingScanner(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }
    // 定义初始扫描模式。
    // 该方法在基类中默认返回 ScanMode.DELTA（增量模式）
    protected ScanMode startingScanMode() {
        return ScanMode.DELTA;
    }

    // 返回任务启动的上下文信息
    @Override
    public StartingContext startingContext() {
        if (startingSnapshotId == null) {
            return StartingContext.EMPTY;
        } else {
            return new StartingContext(startingSnapshotId, startingScanMode() == ScanMode.ALL);
        }
    }
    // 获取启动时涉及到的分区统计信息。
    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        // 调用子类实现的 scan(snapshotReader) 方法来获取扫描结果
        Result result = scan(snapshotReader);
        if (result instanceof ScannedResult) {
            // mergeSplits 的作用是将这些 Split 中的文件元数据进行聚合，提取出受影响的分区（Partition）及其统计信息（如这些分区里有多少文件、多少行）
            return new ArrayList<>(PartitionEntry.mergeSplits(((ScannedResult) result).splits()));
        }
        return Collections.emptyList();
    }
}

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

/** That contains some information that will be used out of StartingScanner. */
// 在 Paimon 中，StartingScanner 负责根据用户的配置（如 latest、from-timestamp 等）计算读取的起点。
// 由于计算过程可能涉及复杂的逻辑（比如处理已过期的快照、寻找最早可用的快照等），
// 计算出的结果需要一种标准化的格式传递给后续的扫描器（如 StreamTableScan），StartingContext 就充当了这个“结果报告”的角色。
// 它解决了两个核心问题：
// 起点定位：到底从哪一个 Snapshot ID 开始读？
// 模式定义：第一次读取时，是把该快照里的所有存量数据都读出来（全量扫描），还是只读该快照产生的增量部分（增量扫描）？
public class StartingContext {
    /**
     * Notice: The snapshot ID is the initial one corresponding to the StartScanner configuration,
     * not necessarily the snapshot ID at the time of the actual scan. E.g, in
     * ContinuousFromSnapshotFullStartingScanner, this snapshot ID used in the first scan is the
     * bigger one between the configured one and the earliest one.
     */
    // 扫描任务启动时的初始快照 ID。
    // 这个 ID 是 StartingScanner 根据配置和实际元数据状态计算出来的逻辑起始点。
    private final Long snapshotId;
    // 标记第一次扫描是否执行全量扫描。
    private final Boolean scanFullSnapshot;

    public StartingContext(Long snapshotId, Boolean scanFullSnapshot) {
        this.snapshotId = snapshotId;
        this.scanFullSnapshot = scanFullSnapshot;
    }

    public Long getSnapshotId() {
        return this.snapshotId;
    }

    public Boolean getScanFullSnapshot() {
        return this.scanFullSnapshot;
    }

    public static final StartingContext EMPTY = new StartingContext(1L, false);
}

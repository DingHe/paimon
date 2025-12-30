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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** A {@link CompactStrategy} to force compacting level 0 files. */
// ForceUpLevel0Compaction 是一个装饰器模式的实现。它封装了标准的 UniversalCompaction 逻辑，并专门针对 Level 0 文件堆积问题引入了“强制触发”机制。
// 确保 Level 0 的文件能够被及时合并到更高层级。
// 在 Paimon 的 LSM-Tree 结构中，Level 0 文件是直接从内存溢写（Flush）产生的，文件之间可能存在大量的键范围重叠。如果 Level 0 文件过多，会导致：
//
//查询性能下降：读取时需要扫描更多的 L0 文件并进行去重。
//
//写放大压力：如果 L0 迟迟不合并，后续的 Flush 可能会被阻塞。
public class ForceUpLevel0Compaction implements CompactStrategy {
    // 持有一个标准的通用合并策略实例。该类的大部分合并决策仍然依赖于 universal 的计算逻辑。
    private final UniversalCompaction universal;
    // 最大合并间隔次数。它定义了“每经过多少次检查，必须强制执行一次合并”。如果为 null，则表示不基于计数器强制触发，而是每次都尝试强制提取 L0。
    @Nullable private final Integer maxCompactInterval;
    // 用于记录自上次合并以来，pick 方法被调用的次数。它会与 maxCompactInterval 进行比较。
    @Nullable private final AtomicInteger compactTriggerCount;

    public ForceUpLevel0Compaction(
            UniversalCompaction universal, @Nullable Integer maxCompactInterval) {
        this.universal = universal;
        this.maxCompactInterval = maxCompactInterval;
        this.compactTriggerCount = maxCompactInterval == null ? null : new AtomicInteger(0);
    }

    @Nullable
    public Integer maxCompactInterval() {
        return maxCompactInterval;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        Optional<CompactUnit> pick = universal.pick(numLevels, runs);
        if (pick.isPresent()) {
            return pick;
        }

        if (maxCompactInterval == null || compactTriggerCount == null) {
            return universal.forcePickL0(numLevels, runs);
        }

        compactTriggerCount.getAndIncrement();
        if (compactTriggerCount.compareAndSet(maxCompactInterval, 0)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Universal compaction due to max lookup compaction interval {}.",
                        maxCompactInterval);
            }
            return universal.forcePickL0(numLevels, runs);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Skip universal compaction due to lookup compaction trigger count {} is less than the max interval {}.",
                        compactTriggerCount.get(),
                        maxCompactInterval);
            }
            return Optional.empty();
        }
    }
}

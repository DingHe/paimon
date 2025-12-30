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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Universal Compaction Style is a compaction style, targeting the use cases requiring lower write
 * amplification, trading off read amplification and space amplification.
 *
 * <p>See RocksDb Universal-Compaction:
 * https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
 */
// UniversalCompaction 类实现了 RocksDB 风格的 通用合并策略 (Universal Compaction)。
// 这是一种非常灵活且高效的合并算法，设计初衷是提供更低的写入放大 (Write Amplification)，并允许用户在读取放大和空间放大之间进行权衡。
// 主要职责是挑选最合适的有序运行 (Sorted Runs) 进行合并
// 与传统的层级合并（Tiered/Leveled）不同，它不强制要求每一层的大小，而是根据以下几个核心条件来触发合并：
// 空间放大触发：当除最后一层外的所有文件大小之和过大时，触发全量合并，释放过期或已删除数据的空间。
// 大小比例触发：如果新产生的较小文件之和与下一个大文件的大小比例满足阈值，则将它们合并。
// 文件数量触发：当分桶内的 SortedRun 数量超过限制时，触发合并以降低读取压力。
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);
    // 空间放大系数阈值（百分比）。如果“非最后一层文件大小之和 / 最后一层大小”超过此比例，触发全量合并。
    private final int maxSizeAmp;
    // 大小比例阈值。用于判断是否将前面较小的若干个 Run 与后面一个较大的 Run 合并。
    private final int sizeRatio;
    // 合并触发数量。当 Run 的总数超过这个值时，强制执行合并决策。
    private final int numRunCompactionTrigger;
    // 用于支持提前触发全量合并的逻辑（例如基于时间或数据量）。
    @Nullable private final EarlyFullCompaction earlyFullCompact;
    // 支持高峰/低峰期策略，在业务低峰期可以设置更激进的合并参数，以牺牲空间利用率换取高性能。
    @Nullable private final OffPeakHours offPeakHours;

    public UniversalCompaction(
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            @Nullable EarlyFullCompaction earlyFullCompact,
            @Nullable OffPeakHours offPeakHours) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.earlyFullCompact = earlyFullCompact;
        this.offPeakHours = offPeakHours;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        // 0 try full compaction by trigger
        if (earlyFullCompact != null) {
            Optional<CompactUnit> unit = earlyFullCompact.tryFullCompact(numLevels, runs);
            if (unit.isPresent()) {
                return unit;
            }
        }

        // 1 checking for reducing size amplification
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 2 checking for size ratio
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 3 checking for file num
        if (runs.size() > numRunCompactionTrigger) {
            // compacting for file num
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty();
    }

    Optional<CompactUnit> forcePickL0(int numLevels, List<LevelSortedRun> runs) {
        // collect all level 0 files
        int candidateCount = 0;
        for (int i = candidateCount; i < runs.size(); i++) {
            if (runs.get(i).level() > 0) {
                break;
            }
            candidateCount++;
        }

        return candidateCount == 0
                ? Optional.empty()
                : Optional.of(pickForSizeRatio(numLevels - 1, runs, candidateCount, true));
    }

    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // size amplification = percentage of additional size
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            if (earlyFullCompact != null) {
                earlyFullCompact.updateLastFullCompaction();
            }
            return CompactUnit.fromLevelRuns(maxLevel, runs);
        }

        return null;
    }

    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        return pickForSizeRatio(maxLevel, runs, 1);
    }

    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        long candidateSize = candidateSize(runs, candidateCount);
        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i);
            if (candidateSize * (100.0 + sizeRatio + ratioForOffPeak()) / 100.0
                    < next.run().totalSize()) {
                break;
            }

            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }

        return null;
    }

    private int ratioForOffPeak() {
        return offPeakHours == null ? 0 : offPeakHours.currentRatio(LocalDateTime.now().getHour());
    }

    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize();
        }
        return size;
    }

    @VisibleForTesting
    CompactUnit createUnit(List<LevelSortedRun> runs, int maxLevel, int runCount) {
        int outputLevel;
        if (runCount == runs.size()) {
            outputLevel = maxLevel;
        } else {
            // level of next run - 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        if (outputLevel == 0) {
            // do not output level 0
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                if (next.level() != 0) {
                    outputLevel = next.level();
                    break;
                }
            }
        }

        if (runCount == runs.size()) {
            if (earlyFullCompact != null) {
                earlyFullCompact.updateLastFullCompaction();
            }
            outputLevel = maxLevel;
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount));
    }
}

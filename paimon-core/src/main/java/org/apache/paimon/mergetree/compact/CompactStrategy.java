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
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Compact strategy to decide which files to select for compaction. */
// 核心任务是决定“哪些文件应该被选中进行合并”
// CompactStrategy 是 Paimon 维持读写平衡的核心大脑。
// 选择权：LSM-Tree 会产生大量层级文件，如果不加管理，读取性能会恶化。该接口的实现类（如 UniversalCompactStrategy 或 SizeTieredCompactStrategy）会根据预设的算法逻辑，从当前的各层文件中挑选出一组最需要合并的文件。
// 合并决策：它决定了是进行增量合并（局部层级合并）还是全量合并（Full Compaction）
// 基于 Run 的管理：Paimon 的合并是基于 SortedRun（有序运行）的，而不是孤立的单个文件（除了 Level 0 比较特殊）。
public interface CompactStrategy {

    Logger LOG = LoggerFactory.getLogger(CompactStrategy.class);

    /**
     * Pick compaction unit from runs.
     *
     * <ul>
     *   <li>compaction is runs-based, not file-based.
     *   <li>level 0 is special, one run per file; all other levels are one run per level.
     *   <li>compaction is sequential from small level to large level.
     * </ul>
     */
    // 用于挑选出本次需要合并的单元（CompactUnit）
    // numLevels：LSM-Tree 的总层数。
    // runs：当前分桶内所有有效的有序运行（Sorted Runs）列表。
    // 合并是基于 Run 的：Level 0 每一个文件被视为一个独立的 Run；而 Level 1 及以上，每一层所有的有序文件集合被视为一个 Run。
    // 顺序性：合并通常是从较小的层级（Level 0, 1...）向较大的层级（Level Max）顺序推进。
    Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs);

    /** Pick a compaction unit consisting of all existing files. */
    // 用于执行全量合并决策（将所有数据合并到最大层）
    // 目的是从当前所有文件中挑选出一个用于全量合并（Full Compaction）的单元。全量合并的目标是将数据最终推向 LSM-Tree 的最大层（Max Level），并进行物理上的数据清理。

    static Optional<CompactUnit> pickFullCompaction(
            int numLevels, // 总层数。
            List<LevelSortedRun> runs, // 当前所有的 Run。
            @Nullable RecordLevelExpire recordLevelExpire, // 数据过期管理器。用于判断文件内的数据是否已过期需要清理。
            @Nullable BucketedDvMaintainer dvMaintainer, // 删除向量（Deletion Vector）维护器。
            boolean forceRewriteAllFiles) { // 是否强制重写所有文件。
        int maxLevel = numLevels - 1;
        // 如果 runs 为空，直接不合并。
        if (runs.isEmpty()) {
            // no sorted run, no need to compact
            return Optional.empty();
        }

        // only max level files
        // 如果当前只有最大层（Max Level）有文件
        if ((runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            List<DataFileMeta> filesToBeCompacted = new ArrayList<>();

            for (DataFileMeta file : runs.get(0).run().files()) {
                // 若 forceRewriteAllFiles 为 true，重写最大层所有文件
                if (forceRewriteAllFiles) {
                    // add all files when force compacted
                    filesToBeCompacted.add(file);
                // 若文件内存在过期数据（isExpireFile），则选中该文件重写。
                } else if (recordLevelExpire != null && recordLevelExpire.isExpireFile(file)) {
                    // check record level expire for large files
                    filesToBeCompacted.add(file);
                // 若文件关联了删除向量（意味着有数据被逻辑删除，重写可以物理删除它们），则选中该文件。
                } else if (dvMaintainer != null
                        && dvMaintainer.deletionVectorOf(file.fileName()).isPresent()) {
                    // check deletion vector for large files
                    filesToBeCompacted.add(file);
                }
            }

            if (filesToBeCompacted.isEmpty()) {
                return Optional.empty();
            }
            // 重写部分文件filesToBeCompacted
            return Optional.of(CompactUnit.fromFiles(maxLevel, filesToBeCompacted, true));
        }

        // full compaction
        // 全量合并触发：如果不仅最大层有数据（即 Level 0 或其他中间层有数据），则将所有层的 Run 全部打包进 CompactUnit，目标直指最大层。
        return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
    }
}

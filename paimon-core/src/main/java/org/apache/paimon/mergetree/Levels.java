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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A class which stores all level files of merge tree. */
// Levels 类是管理 LSM-Tree 结构中所有层级文件的核心组件。它不仅存储了文件的引用，还维护了这些文件的物理层级关系和有序性
// Levels 类在内存中维护了一个 LSM-Tree 的完整视图。
// 层级组织：将数据文件划分为 Level 0 和 Level 1 ~ N。
// 有序性维护：
// Level 0：允许文件之间存在键范围（Key Range）重叠。Levels 内部使用 TreeSet 按照序列号（Sequence Number）由大到小排序，确保读取时“新数据覆盖旧数据”。
// Level 1 及以上：每一层都是一个 SortedRun，即文件之间键范围不重叠且整体有序。
// 动态更新：在 Compaction（合并）完成后，负责更新各层级的文件列表，并触发相关的回调逻辑。
public class Levels {
    // 用于比较 InternalRow 的比较器。
    // 在重建更高层级的 SortedRun 时，用于确保文件按主键顺序排列。
    private final Comparator<InternalRow> keyComparator;
    // 专门存储 Level 0 的文件
    // 排序规则：优先按 maxSequenceNumber 倒序排（新的在前）；若相同，则通过文件名等辅助字段确保 TreeSet 中元素的唯一性。
    private final TreeSet<DataFileMeta> level0;
    // 存储从 Level 1 到 Max Level 的数据。
    // 每个 SortedRun 代表该层级内所有文件的一个有序集合。
    private final List<SortedRun> levels;
    // 当文件因合并而被丢弃（不再被引用）时，通知相关监听者（例如用于清理内存索引或物理删除文件）。
    private final List<DropFileCallback> dropFileCallbacks = new ArrayList<>();

    public Levels(
            Comparator<InternalRow> keyComparator, List<DataFileMeta> inputFiles, int numLevels) {
        // 保存主键比较器。
        // 后续在合并非 0 层（Level > 0）的文件生成 SortedRun 时，需要用它来确保文件是按主键范围有序排列的。
        this.keyComparator = keyComparator;

        // in case the num of levels is not specified explicitly
        // 计算 LSM-Tree 实际拥有的层级数量
        // 确定层级总数
        int restoredNumLevels =
                Math.max(
                        numLevels,
                        inputFiles.stream().mapToInt(DataFileMeta::level).max().orElse(-1) + 1);
        checkArgument(restoredNumLevels > 1, "Number of levels must be at least 2.");
        // 初始化 Level 0 容器。
        // Level 0 允许键范围重叠，因此文件顺序至关重要。
        this.level0 =
                new TreeSet<>(
                        (a, b) -> {
                            // 优先按最大序列号倒序排列。
                            // 这意味着“新”文件排在前面。在读取合并时，排在前面的数据会覆盖后面旧的数据。
                            if (a.maxSequenceNumber() != b.maxSequenceNumber()) {
                                // file with larger sequence number should be in front
                                return Long.compare(b.maxSequenceNumber(), a.maxSequenceNumber());
                            } else {
                                // When two or more jobs are writing the same merge tree, it is
                                // possible that multiple files have the same maxSequenceNumber. In
                                // this case we have to compare their file names so that files with
                                // same maxSequenceNumber won't be "de-duplicated" by the tree set.
                                // 如果最大序列号相同（多作业并发写入可能导致），则比较最小序列号。
                                int minSeqCompare =
                                        Long.compare(a.minSequenceNumber(), b.minSequenceNumber());
                                if (minSeqCompare != 0) {
                                    return minSeqCompare;
                                }
                                // If minSequenceNumber is also the same, use creation time
                                // 如果序列号都一致，则按文件创建时间排序。
                                int timeCompare = a.creationTime().compareTo(b.creationTime());
                                if (timeCompare != 0) {
                                    return timeCompare;
                                }
                                // Final fallback: filename (to ensure uniqueness in TreeSet)
                                // 这是最后的保底方案。TreeSet 如果比较结果返回 0 会去重，
                                // 通过比较唯一的文件名，防止两个不同的文件因为元数据相似而被误认为是同一个。
                                return a.fileName().compareTo(b.fileName());
                            }
                        });
        this.levels = new ArrayList<>();
        // 初始化 Level 1 及以上层级
        for (int i = 1; i < restoredNumLevels; i++) {
            levels.add(SortedRun.empty());
        }
        // 将无序的输入文件列表 inputFiles 按照其所属的 level 字段进行归类，存入一个 Map 中。
        Map<Integer, List<DataFileMeta>> levelMap = new HashMap<>();
        for (DataFileMeta file : inputFiles) {
            levelMap.computeIfAbsent(file.level(), level -> new ArrayList<>()).add(file);
        }
        // 遍历刚才分好组的 Map，调用 updateLevel 方法将文件填入之前初始化的容器中。
        levelMap.forEach((level, files) -> updateLevel(level, emptyList(), files));
        // 验证 level0 里的文件数量加上 levels 列表里所有文件的总和，是否等于初始输入的 inputFiles 数量
        Preconditions.checkState(
                level0.size() + levels.stream().mapToInt(r -> r.files().size()).sum()
                        == inputFiles.size(),
                "Number of files stored in Levels does not equal to the size of inputFiles. This is unexpected.");
    }

    public TreeSet<DataFileMeta> level0() {
        return level0;
    }

    public void addDropFileCallback(DropFileCallback callback) {
        dropFileCallbacks.add(callback);
    }
    // 增加level 0 的文件
    public void addLevel0File(DataFileMeta file) {
        checkArgument(file.level() == 0);
        level0.add(file);
    }
    // 返回level层级的SortedRun
    public SortedRun runOfLevel(int level) {
        checkArgument(level > 0, "Level0 does not have one single sorted run.");
        return levels.get(level - 1);
    }

    public int numberOfLevels() {
        return levels.size() + 1;
    }

    public int maxLevel() {
        return levels.size();
    }

    public int numberOfSortedRuns() {
        int numberOfSortedRuns = level0.size();
        for (SortedRun run : levels) {
            if (run.nonEmpty()) {
                numberOfSortedRuns++;
            }
        }
        return numberOfSortedRuns;
    }

    /** @return the highest non-empty level or -1 if all levels empty. */
    public int nonEmptyHighestLevel() {
        int i;
        for (i = levels.size() - 1; i >= 0; i--) {
            if (levels.get(i).nonEmpty()) {
                return i + 1;
            }
        }
        return level0.isEmpty() ? -1 : 0;
    }

    public long totalFileSize() {
        return level0.stream().mapToLong(DataFileMeta::fileSize).sum()
                + levels.stream().mapToLong(SortedRun::totalSize).sum();
    }

    public List<DataFileMeta> allFiles() {
        List<DataFileMeta> files = new ArrayList<>();
        List<LevelSortedRun> runs = levelSortedRuns();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return files;
    }

    public List<LevelSortedRun> levelSortedRuns() {
        List<LevelSortedRun> runs = new ArrayList<>();
        level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));
        for (int i = 0; i < levels.size(); i++) {
            SortedRun run = levels.get(i);
            if (run.nonEmpty()) {
                runs.add(new LevelSortedRun(i + 1, run));
            }
        }
        return runs;
    }

    public void update(List<DataFileMeta> before, List<DataFileMeta> after) {
        Map<Integer, List<DataFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<DataFileMeta>> groupedAfter = groupByLevel(after);
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }

        if (dropFileCallbacks.size() > 0) {
            Set<String> droppedFiles =
                    before.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
            // exclude upgrade files
            after.stream().map(DataFileMeta::fileName).forEach(droppedFiles::remove);
            for (DropFileCallback callback : dropFileCallbacks) {
                droppedFiles.forEach(callback::notifyDropFile);
            }
        }
    }
    // 更新各个存储级别的文件元数据信息
    private void updateLevel(int level, List<DataFileMeta> before, List<DataFileMeta> after) {
        if (before.isEmpty() && after.isEmpty()) {
            return;
        }
        // 处理 Level 0 (非排序层)
        if (level == 0) {
            before.forEach(level0::remove);
            // 由于 level0 是一个 TreeSet，在调用 addAll 时，它会根据构造函数中定义的“序列号倒序”规则自动进行排序，确保新文件始终排在旧文件前面。
            level0.addAll(after);
        } else {
            // 处理 Level > 0 (有序层)
            // 获取指定层级当前已存在的物理文件列表
            List<DataFileMeta> files = new ArrayList<>(runOfLevel(level).files());
            // 先移除掉被合并掉的旧文件（before），再添加合并后产生的新文件（after）
            files.removeAll(before);
            files.addAll(after);
            // 但 Paimon 要求 Level > 0 的文件必须按主键排序且不能重叠。
            // 该方法会使用 keyComparator 对 files 进行排序，并校验文件之间的键范围（Key Range）是否合法（即不重叠）
            levels.set(level - 1, SortedRun.fromUnsorted(files, keyComparator));
        }
    }

    private Map<Integer, List<DataFileMeta>> groupByLevel(List<DataFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(DataFileMeta::level, Collectors.toList()));
    }

    /** A callback to notify dropping file. */
    public interface DropFileCallback {

        void notifyDropFile(String file);
    }
}

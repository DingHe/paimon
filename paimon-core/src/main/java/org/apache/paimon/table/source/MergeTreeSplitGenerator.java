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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;

/** Merge tree implementation of {@link SplitGenerator}. */
// MergeTreeSplitGenerator 是专门为 Primary Key 表（主键表） 设计的分片生成器实现。由于主键表底层基于 LSM Tree（Merge Tree）结构存储，
// 文件之间可能存在复杂的 Key 范围重叠，因此这个类的核心逻辑是如何在保证正确性（合并重叠数据）的前提下，最大化读取并行度。
// MergeTreeSplitGenerator 的核心作用是执行 “区间分区（Interval Partitioning）” 和 “装箱（Bin Packing）”。
// 区间分区：在 LSM Tree 中，如果两个文件的 Key 范围有重叠，它们必须被分到同一个 Split 中进行归并读取（Merge Read），否则会导致主键合并失效。该类负责识别这些重叠区域（即 Section）。
// 读取优化（Raw Convert）：它会判断哪些文件是不重叠的。如果不重叠，则标记为 rawConvertible，告诉执行引擎可以直接读取物理文件，跳过复杂的归并排序过程。
// 任务负载均衡：利用装箱算法（Bin Packing），将多个小的 Section 组合成一个接近 targetSplitSize 的 Split，避免产生过多的小任务。
public class MergeTreeSplitGenerator implements SplitGenerator {
    // 主键比较器。
    // 用于确定文件的 Key 范围是否重叠。
    private final Comparator<InternalRow> keyComparator;
    // 目标分片大小。
    // 控制一个 Split 包含多少字节的数据，对应配置项 read.split.target-size。
    private final long targetSplitSize;
    // 开销权重。
    // 打开一个文件的额外开销（折算成字节数），防止产生包含大量极小文件的 Split。
    private final long openFileCost;
    // 是否启用删除向量。
    // 如果启用，即使文件有重叠，也可以通过 DV 快速过滤，有助于实现 Raw Convert。
    private final boolean deletionVectorsEnabled;
    // 合并引擎类型（如 DEDUPLICATE, FIRST_ROW）。
    // 不同的引擎对“是否必须合并”的要求不同。
    private final MergeEngine mergeEngine;

    public MergeTreeSplitGenerator(
            Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine) {
        this.keyComparator = keyComparator;
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
    }
    // 判断该配置下是否所有 Split 理论上都可以直接读取。
    // 如果开启了 Deletion Vectors（删除向量）或者合并引擎是 FIRST_ROW（只取第一行），由于其特殊的物理处理方式，通常可以直接进行原始读取。
    @Override
    public boolean alwaysRawConvertible() {
        return deletionVectorsEnabled || mergeEngine == FIRST_ROW;
    }
    // Paimon 主键表在批处理模式下计算分片的“大脑”
    // 核心逻辑是：在保证主键合并正确性的前提下，尽可能通过并行化和“直读优化（Raw Convert）”来提升速度。
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        // file.level() != 0：在 LSM Tree 中，Level 0 的文件是根据时间顺序刷写的，Key 范围高度重叠。
        // Level > 0 的文件在层内是保证不重叠的。
        // withoutDeleteRow(file)：检查文件是否包含删除记录。如果包含删除记录，必须经过合并逻辑处理才能得到正确结果。
        boolean rawConvertible =
                files.stream().allMatch(file -> file.level() != 0 && withoutDeleteRow(file));
        // 判断所有文件是否都处于同一层（Level）
        boolean oneLevel =
                files.stream().map(DataFileMeta::level).collect(Collectors.toSet()).size() == 1;
        // 如果满足以下任一条件，则进入“高性能直读”分片逻辑：
        // 启用了 Deletion Vectors（删除向量，可抵消重叠影响）。
        // 使用 FIRST_ROW 合并引擎（只关心第一行，逻辑简单）
        // 所有文件都在同一个非零层（绝对不重叠）
        if (rawConvertible && (deletionVectorsEnabled || mergeEngine == FIRST_ROW || oneLevel)) {
            // 定义计算文件“权重”的函数
            Function<DataFileMeta, Long> weightFunc =
                    file -> Math.max(file.fileSize(), openFileCost);
            // 将有序的文件列表切分成多个组，使每组总大小接近 targetSplitSize。
            return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                    // 将这些组直接标记为 Raw Convertible。这意味着执行引擎读取时不需要做归并排序，速度最快。
                    .map(SplitGroup::rawConvertibleGroup)
                    .collect(Collectors.toList());
        }

        /*
         * The generator aims to parallel the scan execution by slicing the files of each bucket
         * into multiple splits. The generation has one constraint: files with intersected key
         * ranges (within one section) must go to the same split. Therefore, the files are first to go
         * through the interval partition algorithm to generate sections and then through the
         * OrderedPack algorithm. Note that the item to be packed here is each section, the capacity
         * is denoted as the targetSplitSize, and the final number of the bins is the number of
         * splits generated.
         *
         * For instance, there are files: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]
         * with targetSplitSize 128M. After interval partition, there are four sections:
         * - section1: [1, 2]
         * - section2: [3, 4]
         * - section3: [5, 180], [5, 190]
         * - section4: [200, 600], [210, 700]
         *
         * After OrderedPack, section1 and section2 will be put into one bin (split), so the final result will be:
         * - split1: [1, 2] [3, 4]
         * - split2: [5, 180] [5,190]
         * - split3: [200, 600] [210, 700]
         */
        // 如果无法走快速路径，说明文件之间可能存在复杂的 Key 重叠。
        // 这是主键表最核心的逻辑——区间分区
        // 原理：IntervalPartition 会扫描所有文件的 Key 范围（Min-Max）。
        // 如果文件 A 和文件 B 的 Key 范围有交集，它们就会被强行捆绑在一起形成一个 Section。
        // 一个 Section 代表了一组“必须放在一起处理”的文件，否则主键合并就会出错。
        List<List<DataFileMeta>> sections =
                new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());
        // 将上一步生成的 sections 再次进行装箱。
        // sections 是逻辑上的最小单元。这一步是将多个小的 section 打包成一个物理上的 Split，以满足用户设定的 targetSplitSize。
        // 如果一个分片内只有一个文件且没有删除记录，说明它是干净的，标记为 rawConvertibleGroup（直读）。
        // 否则，标记为 nonRawConvertibleGroup。执行引擎在读取此类分片时，会启动 MergeTree 归并排序逻辑，以确保主键去重或聚合。
        return packSplits(sections).stream()
                .map(
                        f ->
                                f.size() == 1 && withoutDeleteRow(f.get(0))
                                        ? SplitGroup.rawConvertibleGroup(f)
                                        : SplitGroup.nonRawConvertibleGroup(f))
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // We don't split streaming scan files
        return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
    }

    private List<List<DataFileMeta>> packSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return BinPacking.packForOrdered(sections, weightFunc, targetSplitSize).stream()
                .map(this::flatFiles)
                .collect(Collectors.toList());
    }

    private long totalSize(List<DataFileMeta> section) {
        long size = 0L;
        for (DataFileMeta file : section) {
            size += file.fileSize();
        }
        return size;
    }

    private List<DataFileMeta> flatRun(List<SortedRun> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(run -> files.addAll(run.files()));
        return files;
    }

    private List<DataFileMeta> flatFiles(List<List<DataFileMeta>> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(files::addAll);
        return files;
    }
    // 判断数据文件是否包含删除记录
    private boolean withoutDeleteRow(DataFileMeta dataFileMeta) {
        // null to true to be compatible with old version
        return dataFileMeta.deleteRowCount().map(count -> count == 0L).orElse(true);
    }
}

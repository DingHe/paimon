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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.manifest.ManifestEntry.recordCount;

/** Scan operation which produces a plan. */
// FileStoreScan 是一个处于核心地位的接口。如果说 SnapshotReader 是面向用户的“查询规划器”，那么 FileStoreScan 就是底层的**“物理清单扫描引擎”**。
// 遍历元数据清单（Manifests），并筛选出符合条件的物理文件列表。
// 在 Paimon 这种基于 LSM 树结构的存储格式中，数据并不是存储在单一文件中，而是分布在大量的 DataFile 中。FileStoreScan 的作用包括：
// 物理过滤：根据分区、桶（Bucket）、LSM 层级（Level）以及列统计信息（Stats）对物理文件进行初筛。
// 版本定位：根据特定的快照（Snapshot）ID，找到对应的时间点视图。
// 生成执行计划：最终产出一个 Plan 对象，里面包含了所有需要被读取的 ManifestEntry（即文件元数据条目）。
public interface FileStoreScan {
    // 分区过滤
    // 通过传入谓词（Predicate）或具体分区值，直接跳过不相关的分区文件夹。
    FileStoreScan withPartitionFilter(Predicate predicate);

    FileStoreScan withPartitionFilter(List<BinaryRow> partitions);

    FileStoreScan withPartitionsFilter(List<Map<String, String>> partitions);

    FileStoreScan withPartitionFilter(PartitionPredicate predicate);
    // 桶过滤
    FileStoreScan withBucket(int bucket);

    FileStoreScan onlyReadRealBuckets();
    // 桶过滤
    FileStoreScan withBucketFilter(Filter<Integer> bucketFilter);

    FileStoreScan withTotalAwareBucketFilter(BiFilter<Integer, Integer> bucketFilter);
    // 桶过滤
    FileStoreScan withPartitionBucket(BinaryRow partition, int bucket);
    // 指定扫描哪一个版本的快照。
    FileStoreScan withSnapshot(long snapshotId);

    FileStoreScan withSnapshot(Snapshot snapshot);
    // 指定扫描模式，如 ALL（全量扫描快照内容）或 DELTA（只扫描该快照产生的增量变化）。
    FileStoreScan withKind(ScanMode scanMode);
    // 层级过滤
    FileStoreScan withLevel(int level);
    // 层级过滤
    FileStoreScan withLevelFilter(Filter<Integer> levelFilter);
    // 层级过滤
    FileStoreScan withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter);
    // 启用后，会利用文件统计信息（Min/Max）对数据文件进行物理层面的裁剪（Skipping）。
    FileStoreScan enableValueFilter();
    // 文件过滤
    FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter);
    // 文件过滤
    FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter);

    FileStoreScan withMetrics(ScanMetrics metrics);
    // 决定是否在内存中保留列统计信息。丢弃统计信息可以显著减少 Scan 过程中的内存消耗，但会失去根据查询条件裁剪文件的能力。
    FileStoreScan dropStats();

    FileStoreScan keepStats();

    FileStoreScan withRowRanges(List<Range> rowRanges);

    FileStoreScan withReadType(RowType readType);
    // 限制扫描的文件或记录条数
    FileStoreScan withLimit(long limit);

    @Nullable
    Integer parallelism();
    // 获取用于读取 manifest-xxx 文件的底层读取器
    ManifestsReader manifestsReader();
    // 具体读取某一个清单文件中的所有条目
    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    /** Produce a {@link Plan}. */
    // 最核心的方法
    // 触发实际的清单扫描逻辑，返回一个 Plan
    Plan plan();

    /**
     * Return record count of all changes occurred in this snapshot given the scan.
     *
     * @return total record count of Snapshot.
     */
    default Long totalRecordCount(Snapshot snapshot) {
        return snapshot.totalRecordCount() == null
                ? (Long) recordCount(withSnapshot(snapshot.id()).plan().files())
                : snapshot.totalRecordCount();
    }

    /**
     * Read {@link SimpleFileEntry}s, SimpleFileEntry only retains some critical information, so it
     * cannot perform filtering based on statistical information.
     */
    List<SimpleFileEntry> readSimpleEntries();
    // 返回分区或桶的汇总统计信息（用于查询优化器评估代价）。
    List<PartitionEntry> readPartitionEntries();

    List<BucketEntry> readBucketEntries();

    Iterator<ManifestEntry> readFileIterator();
    // 通过扫描元数据，快速列出表中当前存在的所有分区。
    default List<BinaryRow> listPartitions() {
        return readPartitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toList());
    }

    /** Result plan of this scan. */
    // 扫描操作的结果，它定义了查询引擎接下来需要处理的物理范围：
    interface Plan {
        // 返回该计划所属的快照版本和水位线。
        @Nullable
        Long watermark();

        /**
         * Snapshot of this plan, return null if the table is empty or the manifest list is
         * specified.
         */
        @Nullable
        Snapshot snapshot();

        /** Result {@link ManifestEntry} files. */
        // 返回所有匹配的 ManifestEntry 列表。
        // 每个条目代表一个物理文件及其元数据。
        List<ManifestEntry> files();

        /** Result {@link ManifestEntry} files with specific file kind. */
        // 根据文件类型（ADD 增加或 DELETE 删除）进行过滤。在处理变更流（Changelog）时非常重要。
        default List<ManifestEntry> files(FileKind kind) {
            return files().stream().filter(e -> e.kind() == kind).collect(Collectors.toList());
        }

        /** Return a map group by partition and bucket. */
        // 将散乱的文件列表按照 分区 -> 桶 的层级重新组织成一个嵌套的 Map。
        static Map<BinaryRow, Map<Integer, List<ManifestEntry>>> groupByPartFiles(
                List<ManifestEntry> files) {
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> groupBy = new LinkedHashMap<>();
            for (ManifestEntry entry : files) {
                groupBy.computeIfAbsent(entry.partition(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                        .add(entry);
            }
            return groupBy;
        }
    }
}

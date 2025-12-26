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
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;

/** A util class to read manifest files. */
// ManifestsReader 是一个**负责解析元数据并进行初步过滤（Pruning）**的核心工具类。它处于快照（Snapshot）和具体数据文件（Data File）之间，起到“承上启下”的作用。
// ManifestsReader 的主要作用是读取并筛选清单文件（Manifest Files）
// 在 Paimon 中，一个 Snapshot 包含多个 Manifest File，每个 Manifest File 记录了一组数据文件的变更。ManifestsReader 的价值在于：
// 元数据检索：根据 ScanMode（全量、增量或变更日志）从指定的 Snapshot 中提取出清单列表。
// 高效裁剪（Filtering/Pruning）：在读取庞大的元数据之前，利用清单文件元数据（ManifestFileMeta）中记录的统计信息（如分区范围、Bucket 范围、Level 范围）进行过滤，从而避免读取不相关的元数据文件，极大提升查询效率。
// 流式与批式统一：支持不同模式的扫描需求。
@ThreadSafe
public class ManifestsReader {
    // 分区字段的类型信息，用于构建分区过滤器。
    private final RowType partitionType;
    // 分区的默认值，处理分区字段为空或默认的情况。
    private final String partitionDefaultValue;
    // 用于获取快照信息（如最新的 Snapshot）。
    private final SnapshotManager snapshotManager;
    // 工厂类，用于创建 ManifestList 对象来实际读取物理文件。
    private final ManifestList.Factory manifestListFactory;
    // 是否只读取真实的 Bucket（Bucket >= 0）。在某些动态 Bucket 场景下会用到。
    private boolean onlyReadRealBuckets = false;
    // 指定只读取某个特定的 Bucket。
    @Nullable private Integer specifiedBucket = null;
    // 指定只读取 LSM Tree 中特定层级（Level）的文件清单。
    @Nullable private Integer specifiedLevel = null;
    // 分区过滤器，基于分区统计信息进行物理裁剪。
    @Nullable private PartitionPredicate partitionFilter = null;
    // 层级过滤器，根据清单中记录的最小/最大层级进行筛选。
    @Nullable private BiFilter<Integer, Integer> levelMinMaxFilter = null;

    public ManifestsReader(
            RowType partitionType,
            String partitionDefaultValue,
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory) {
        this.partitionType = partitionType;
        this.partitionDefaultValue = partitionDefaultValue;
        this.snapshotManager = snapshotManager;
        this.manifestListFactory = manifestListFactory;
    }

    public ManifestsReader onlyReadRealBuckets() {
        this.onlyReadRealBuckets = true;
        return this;
    }

    public ManifestsReader withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    public ManifestsReader withLevel(int level) {
        this.specifiedLevel = level;
        return this;
    }

    public ManifestsReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        this.levelMinMaxFilter = minMaxFilter;
        return this;
    }

    public ManifestsReader withPartitionFilter(Predicate predicate) {
        this.partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        return this;
    }

    public ManifestsReader withPartitionFilter(List<BinaryRow> partitions) {
        this.partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
        return this;
    }

    public ManifestsReader withPartitionsFilter(List<Map<String, String>> partitions) {
        return withPartitionFilter(
                createBinaryPartitions(partitions, partitionType, partitionDefaultValue));
    }

    public ManifestsReader withPartitionFilter(PartitionPredicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    @Nullable
    public PartitionPredicate partitionFilter() {
        return partitionFilter;
    }
    // 如果未指定 Snapshot，则自动获取 latestSnapshot()
    public Result read(@Nullable Snapshot specifiedSnapshot, ScanMode scanMode) {
        List<ManifestFileMeta> manifests;
        Snapshot snapshot =
                specifiedSnapshot == null ? snapshotManager.latestSnapshot() : specifiedSnapshot;
        if (snapshot == null) {
            manifests = Collections.emptyList();
        } else {
            // 调用 readManifests 根据 ScanMode（ALL/DELTA/CHANGELOG）加载所有的清单元数据。
            manifests = readManifests(snapshot, scanMode);
        }

        List<ManifestFileMeta> filtered =
                manifests.stream()
                        .filter(this::filterManifestFileMeta)
                        .collect(Collectors.toList());
        return new Result(snapshot, manifests, filtered);
    }

    private List<ManifestFileMeta> readManifests(Snapshot snapshot, ScanMode scanMode) {
        ManifestList manifestList = manifestListFactory.create();
        switch (scanMode) {
            case ALL:
                return manifestList.readDataManifests(snapshot);
            case DELTA:
                return manifestList.readDeltaManifests(snapshot);
            case CHANGELOG:
                if (snapshot.version() <= Snapshot.TABLE_STORE_02_VERSION) {
                    throw new UnsupportedOperationException(
                            "Unsupported snapshot version: " + snapshot.version());
                }
                return manifestList.readChangelogManifests(snapshot);
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanMode.name());
        }
    }

    /** Note: Keep this thread-safe. */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        Integer minBucket = manifest.minBucket();
        Integer maxBucket = manifest.maxBucket();
        if (minBucket != null && maxBucket != null) {
            if (onlyReadRealBuckets && maxBucket < 0) {
                return false;
            }
            if (specifiedBucket != null
                    && (specifiedBucket < minBucket || specifiedBucket > maxBucket)) {
                return false;
            }
        }

        Integer minLevel = manifest.minLevel();
        Integer maxLevel = manifest.maxLevel();
        if (minLevel != null && maxLevel != null) {
            if (specifiedLevel != null
                    && (specifiedLevel < minLevel || specifiedLevel > maxLevel)) {
                return false;
            }
            if (levelMinMaxFilter != null && !levelMinMaxFilter.test(minLevel, maxLevel)) {
                return false;
            }
        }

        if (partitionFilter == null) {
            return true;
        }

        SimpleStats stats = manifest.partitionStats();
        return partitionFilter == null
                || partitionFilter.test(
                        manifest.numAddedFiles() + manifest.numDeletedFiles(),
                        stats.minValues(),
                        stats.maxValues(),
                        stats.nullCounts());
    }

    /** Result for reading manifest files. */
    public static final class Result {

        public final Snapshot snapshot;
        public final List<ManifestFileMeta> allManifests;
        public final List<ManifestFileMeta> filteredManifests;

        public Result(
                Snapshot snapshot,
                List<ManifestFileMeta> allManifests,
                List<ManifestFileMeta> filteredManifests) {
            this.snapshot = snapshot;
            this.allManifests = allManifests;
            this.filteredManifests = filteredManifests;
        }
    }

    public static Result emptyResult() {
        return new Result(null, Collections.emptyList(), Collections.emptyList());
    }
}

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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;

/** Implementation of {@link SnapshotReader}. */
// Paimon 读取流程中的核心控制器，负责将复杂的物理存储元数据（快照、索引、LSM 文件）翻译成计算引擎可识别的逻辑分片（Splits）。
// 执行扫描计划（Scanning）：它通过内部的 FileStoreScan 找到特定快照下所有的 Manifest 记录。
// 处理删除向量（Deletion Vectors）：它是处理 Paimon 0.8+ 引入的 DV 索引的核心类，负责读取 .dv 文件元数据并将其挂载到数据分片上。
// 实现多种扫描模式：它支持 ALL（全量扫描）、DELTA（增量扫描）和 DIFF（快照间差异扫描）。
// 优化与分片（Split Generation）：它利用 SplitGenerator 将分布在不同 Bucket 里的文件，根据 LSM 树的结构（如 SortedRun）组合成最优的 DataSplit。


public class SnapshotReaderImpl implements SnapshotReader {
    // 核心扫描引擎，
    // 负责最底层的清单（Manifest）遍历逻辑。
    private final FileStoreScan scan;
    // 当前表的结构定义，用于解析分区字段和行类型。
    private final TableSchema tableSchema;
    // 表的配置项，例如是否启用删除向量、分区排序策略等。
    private final CoreOptions options;
    private final boolean deletionVectors;
    // 用于查找和加载特定的快照或变更日志文件。
    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final ConsumerManager consumerManager;
    // 分片生成器，决定如何将多个文件打包成一个 DataSplit（批模式或流模式）。
    private final SplitGenerator splitGenerator;
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    // 负责生成文件系统的物理路径（如桶目录路径）
    private final FileStorePathFactory pathFactory;
    private final String tableName;
    // 处理 Paimon 的索引文件（如 Hash 索引、Deletion Vector 索引）
    private final IndexFileHandler indexFileHandler;
    // 重要优化点。缓存删除向量（Deletion Vector）的元数据，避免重复读取索引文件产生高 IO。
    @Nullable private final DVMetaCache dvMetaCache;
    // 当前的扫描模式（ALL, DELTA 等）
    private ScanMode scanMode = ScanMode.ALL;
    private RecordComparator lazyPartitionComparator;
    // 用于监控 DV 缓存的命中率
    private CacheMetrics dvMetaCacheMetrics;

    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            FileStorePathFactory pathFactory,
            String tableName,
            IndexFileHandler indexFileHandler,
            @Nullable DVMetaCache dvMetaCache) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.deletionVectors = options.deletionVectorsEnabled();
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.pathFactory = pathFactory;

        this.tableName = tableName;
        this.indexFileHandler = indexFileHandler;
        this.dvMetaCache = dvMetaCache;
    }

    @Override
    public Integer parallelism() {
        return scan.parallelism();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    @Override
    public ChangelogManager changelogManager() {
        return changelogManager;
    }

    @Override
    public ManifestsReader manifestsReader() {
        return scan.manifestsReader();
    }

    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return scan.readManifest(manifest);
    }

    @Override
    public ConsumerManager consumerManager() {
        return consumerManager;
    }

    @Override
    public SplitGenerator splitGenerator() {
        return splitGenerator;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public SnapshotReader withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public SnapshotReader withSnapshot(Snapshot snapshot) {
        scan.withSnapshot(snapshot);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            Predicate partitionPredicate =
                    createPartitionPredicate(
                            partitionSpec,
                            tableSchema.logicalPartitionType(),
                            options.partitionDefaultName());
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(Predicate predicate) {
        scan.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(List<BinaryRow> partitions) {
        scan.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate) {
        if (partitionPredicate != null) {
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions) {
        if (partitions != null) {
            scan.withPartitionsFilter(partitions);
        }
        return this;
    }

    @Override
    public SnapshotReader withFilter(Predicate predicate) {
        Pair<Optional<PartitionPredicate>, List<Predicate>> pair =
                splitPartitionPredicatesAndDataPredicates(
                        predicate, tableSchema.logicalRowType(), tableSchema.partitionKeys());
        if (pair.getLeft().isPresent()) {
            scan.withPartitionFilter(pair.getLeft().get());
        }
        if (!pair.getRight().isEmpty()) {
            nonPartitionFilterConsumer.accept(scan, PredicateBuilder.and(pair.getRight()));
        }
        return this;
    }

    @Override
    public SnapshotReader withMode(ScanMode scanMode) {
        this.scanMode = scanMode;
        scan.withKind(scanMode);
        return this;
    }

    @Override
    public SnapshotReader withLevel(int level) {
        scan.withLevel(level);
        return this;
    }

    @Override
    public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public SnapshotReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        scan.withLevelMinMaxFilter(minMaxFilter);
        return this;
    }

    @Override
    public SnapshotReader enableValueFilter() {
        scan.enableValueFilter();
        return this;
    }

    @Override
    public SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter) {
        scan.withManifestEntryFilter(filter);
        return this;
    }

    @Override
    public SnapshotReader withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
    }

    @Override
    public SnapshotReader onlyReadRealBuckets() {
        scan.onlyReadRealBuckets();
        return this;
    }

    @Override
    public SnapshotReader withBucketFilter(Filter<Integer> bucketFilter) {
        scan.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public SnapshotReader withMetricRegistry(MetricRegistry registry) {
        ScanMetrics scanMetrics = new ScanMetrics(registry, tableName);
        dvMetaCacheMetrics = scanMetrics.getDvMetaCacheMetrics();
        scan.withMetrics(scanMetrics);
        return this;
    }

    @Override
    public SnapshotReader withRowRanges(List<Range> rowRanges) {
        scan.withRowRanges(rowRanges);
        return this;
    }

    @Override
    public SnapshotReader withReadType(RowType readType) {
        scan.withReadType(readType);
        return this;
    }

    @Override
    public SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter) {
        scan.withDataFileNameFilter(fileNameFilter);
        return this;
    }

    @Override
    public SnapshotReader withLimit(int limit) {
        scan.withLimit(limit);
        return this;
    }

    @Override
    public SnapshotReader dropStats() {
        scan.dropStats();
        return this;
    }

    @Override
    public SnapshotReader keepStats() {
        scan.keepStats();
        return this;
    }

    @Override
    public SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        if (splitGenerator.alwaysRawConvertible()) {
            withDataFileNameFilter(
                    file ->
                            Math.abs(file.hashCode() % numberOfParallelSubtasks)
                                    == indexOfThisSubtask);
        } else {
            withBucketFilter(bucket -> bucket % numberOfParallelSubtasks == indexOfThisSubtask);
        }
        return this;
    }

    /** Get splits from {@link FileKind#ADD} files. */
    // Apache Paimon 在进行数据读取（扫描）时的核心入口。
    // 它的主要任务是根据扫描策略，将底层的 数据文件（Data Files） 转化为计算引擎（如 Flink/Spark）可以并行处理的 逻辑分片（Splits）。
    @Override
    public Plan read() {
        // 调用底层的 FileStoreScan 来获取一个初步的扫描计划
        // scan.plan() 会解析元数据（Manifest 文件），确定哪些文件属于当前要读取的范围。
        FileStoreScan.Plan plan = scan.plan();
        @Nullable Snapshot snapshot = plan.snapshot();
        // 将扫描到的“新增类型”（FileKind.ADD）文件按照 分区（Partition） 和 桶（Bucket） 进行分层归类。
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> grouped =
                groupByPartFiles(plan.files(FileKind.ADD));
        // 如果配置了 scan.plan-sort-partition，则对分区进行排序。
        // 默认情况下，Map 的顺序是随机的。但在某些场景（如按时间分区读取）下，按分区顺序处理数据可以提高计算引擎的缓存命中率或满足顺序产出需求。
        if (options.scanPlanSortPartition()) {
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> sorted = new LinkedHashMap<>();
            grouped.entrySet().stream()
                    .sorted((o1, o2) -> partitionComparator().compare(o1.getKey(), o2.getKey()))
                    .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
            grouped = sorted;
        }
        // 将分组后的文件物理信息转化为可分发的 DataSplit 列表
        List<DataSplit> splits =
                generateSplits(snapshot, scanMode != ScanMode.ALL, splitGenerator, grouped);
        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }
    // Apache Paimon 将底层的物理文件元数据转换为计算引擎可识别的逻辑分片（Splits）的核心逻辑。
    // 它不仅负责文件的组合，还处理了 Paimon 的高级特性：删除向量（Deletion Vectors）。
    private List<DataSplit> generateSplits(
            @Nullable Snapshot snapshot,
            boolean isStreaming,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> entries) {
        // 初始化结果列表，并根据是否为批处理模式（!isStreaming）决定是否加载删除文件（Deletion Files）。
        // Deletion Vectors (DV)：Paimon 支持通过删除向量实现高效更新。
        // 在批读时，一次性调用 scanDvIndex 读取所有相关的删除索引，可以显著减少磁盘 IO。
        // 如果是流式读取（Streaming），通常处理的是增量 Changelog，一般不需要处理全量的删除向量。
        List<DataSplit> splits = new ArrayList<>();
        // Read deletion indexes at once to reduce file IO
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFilesMap = null;
        if (!isStreaming) {
            deletionFilesMap =
                    deletionVectors && snapshot != null
                            ? scanDvIndex(snapshot, toPartBuckets(entries))
                            : Collections.emptyMap();
        }
        // 层层解析传入的元数据。
        // Paimon 的数据组织结构是 Partition -> Bucket -> Data Files。这里通过嵌套循环，定位到具体的每一个桶（Bucket）及其包含的文件列表。
        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> entry : entries.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<ManifestEntry>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry : buckets.entrySet()) {
                // 提取桶内文件
                int bucket = bucketEntry.getKey();
                // 将清单项（ManifestEntry）转换为具体的数据文件元数据（DataFileMeta）
                List<DataFileMeta> bucketFiles =
                        bucketEntry.getValue().stream()
                                .map(ManifestEntry::file)
                                .collect(Collectors.toList());
                // 构建 DataSplit 基础信息
                // 记录该分片所属的快照 ID、分区、桶 ID 以及总桶数，这些信息对计算引擎的任务调度非常重要。
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(
                                        snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id())
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withTotalBuckets(bucketEntry.getValue().get(0).totalBuckets())
                                .isStreaming(isStreaming);
                // 调用分片策略（核心算法）
                // 根据读取模式，决定如何将一个桶内的多个文件“打包”成组。
                // 批处理模式：可能会将多个小文件合并为一个 SplitGroup 以减少 Task 数量。
                // 流处理模式：通常需要保持文件的顺序或特定的增量逻辑。
                List<SplitGenerator.SplitGroup> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(bucketFiles)
                                : splitGenerator.splitForBatch(bucketFiles);
                // 生成最终分片并绑定删除文件
                // 遍历切分后的组，设置数据文件列表和物理路径。
                for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                    List<DataFileMeta> dataFiles = splitGroup.files;
                    String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                    builder.withDataFiles(dataFiles)
                            .rawConvertible(splitGroup.rawConvertible)
                            .withBucketPath(bucketPath);
                    // 如果开启了删除向量，将该分片内文件对应的 DeletionFile 映射到 Split 中。
                    // 读取引擎在读取 dataFiles 时，会根据这里绑定的 DeletionFile 过滤掉已被删除的行，实现 Merge-on-Read 之后的结果返回。
                    if (deletionVectors && deletionFilesMap != null) {
                        builder.withDataDeletionFiles(
                                getDeletionFiles(
                                        dataFiles,
                                        deletionFilesMap.getOrDefault(
                                                Pair.of(partition, bucket),
                                                Collections.emptyMap())));
                    }
                    splits.add(builder.build());
                }
            }
        }
        return splits;
    }

    @Override
    public List<BinaryRow> partitions() {
        return scan.listPartitions();
    }

    @Override
    public List<PartitionEntry> partitionEntries() {
        return scan.readPartitionEntries();
    }

    @Override
    public List<BucketEntry> bucketEntries() {
        return scan.readBucketEntries();
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator() {
        return scan.readFileIterator();
    }
    // readChanges 方法是 Apache Paimon 用于**增量读取（Incremental Read）**的核心逻辑。
    // 它的目的是找出两个快照（Snapshot）之间发生的变化，通常用于流式消费（Streaming Read）场景。
    @Override
    public Plan readChanges() {
        // 设置扫描模式为增量（DELTA）
        withMode(ScanMode.DELTA);
        // 通过清单文件（Manifests）计算出本次增量扫描涉及到的文件变更。
        // 由于设置了 DELTA 模式，plan 会包含两个版本之间被删除（DELETE）的文件和新增（ADD）的文件。
        FileStoreScan.Plan plan = scan.plan();
        // 获取所有标记为 DELETE 的文件，并按分区（Partition）和桶（Bucket）进行分组。
        // 在增量读取中，FileKind.DELETE 代表了变更记录中的“旧状态”（UPDATE_BEFORE 或 DELETE）。
        // 通过 groupByPartFiles 将这些文件整理成 Map<分区, Map<Bucket, 文件列表>> 的结构。
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles =
                groupByPartFiles(plan.files(FileKind.DELETE));
        // 获取所有标记为 ADD 的文件，并同样按分区和桶进行分组。
        // FileKind.ADD 代表了变更记录中的“新状态”（INSERT 或 UPDATE_AFTER）
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        // 创建一个懒加载对象，指向当前快照的前一个快照（id - 1）
        // 增量计算通常需要对比当前快照和前一个快照。
        LazyField<Snapshot> beforeSnapshot =
                new LazyField<>(() -> snapshotManager.snapshot(plan.snapshot().id() - 1));
        // 转换为最终的变更计划
        return toChangesPlan(true, plan, beforeSnapshot, beforeFiles, dataFiles);
    }

    // Apache Paimon 处理增量数据的“总装配线”。
    // 它的作用是对比两个快照版本之间的文件差异，并将这些差异封装成包含“变更前”和“变更后”状态的 DataSplit，从而让上游能够感知到数据的具体变化（如更新和删除）。

    private Plan toChangesPlan(
            boolean isStreaming,
            FileStoreScan.Plan plan,
            LazyField<Snapshot> beforeSnapshot,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> dataFiles) {
        Snapshot snapshot = plan.snapshot();
        List<DataSplit> splits = new ArrayList<>();
        Map<BinaryRow, Set<Integer>> buckets = new HashMap<>();
        //  收集所有涉及变更的分区和桶
        // 确定哪些分区和桶发生了数据变动。
        // 通过遍历 beforeFiles（旧文件）和 dataFiles（新文件），汇总出一张“变动清单” buckets。
        // 只要一个桶在旧版本或新版本中出现了文件变化，它就需要被处理。
        beforeFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        dataFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        // Read deletion indexes at once to reduce file IO
        // 批量读取删除向量（Deletion Vectors）索引
        // 如果开启了删除向量（DV）且非流式模式，提前加载所有相关的“删除黑名单”。
        // 为了支持高效的 Merge-on-Read，这里分别获取了变更前后的删除状态映射。
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> beforeDeletionFilesMap = null;
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFilesMap = null;
        if (!isStreaming && deletionVectors) {
            //  读取旧快照的删除索引
            beforeDeletionFilesMap =
                    beforeSnapshot.get() != null
                            ? scanDvIndex(beforeSnapshot.get(), toPartBuckets(beforeFiles))
                            : Collections.emptyMap();
            // // 读取当前新快照的删除索引
            deletionFilesMap =
                    snapshot != null
                            ? scanDvIndex(snapshot, toPartBuckets(dataFiles))
                            : Collections.emptyMap();
        }
        // 处理每个桶的文件差异（去重逻辑）
        // 对比新旧文件列表，剔除没有发生变化的文件。
        // 这是增量读取的关键。如果一个文件同时出现在 beforeEntries 和 dataEntries 中，说明这个文件在两个快照间没动过。
        // removeIf(dataEntries::remove) 会把完全相同的文件记录从新旧列表中同时删掉，只留下真正新增或真正删除的文件
        for (Map.Entry<BinaryRow, Set<Integer>> entry : buckets.entrySet()) {
            BinaryRow part = entry.getKey();
            for (Integer bucket : entry.getValue()) {
                //  获取该桶旧文件
                List<ManifestEntry> beforeEntries =
                        beforeFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());
                // // 获取该桶新文件
                List<ManifestEntry> dataEntries =
                        dataFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());

                // deduplicate
                beforeEntries.removeIf(dataEntries::remove);
                // 提取文件元数据与构建 Split
                Integer totalBuckets = null;
                if (!dataEntries.isEmpty()) {
                    totalBuckets = dataEntries.get(0).totalBuckets();
                } else if (!beforeEntries.isEmpty()) {
                    totalBuckets = beforeEntries.get(0).totalBuckets();
                }

                List<DataFileMeta> before =
                        beforeEntries.stream()
                                .map(ManifestEntry::file)
                                .collect(Collectors.toList());
                List<DataFileMeta> data =
                        dataEntries.stream().map(ManifestEntry::file).collect(Collectors.toList());
                // 组装 DataSplit
                // 读取引擎拿到这个 Split 后，会先读 before 产生 -U（UpdateBefore）或 D（Delete）消息，再读 data 产生 +U（UpdateAfter）或 I（Insert）消息。
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(snapshot.id())
                                .withPartition(part)
                                .withBucket(bucket)
                                .withTotalBuckets(totalBuckets)
                                // 变更前的文件（标记为 DELETE）
                                .withBeforeFiles(before)
                                // // 变更后的文件（标记为 ADD）
                                .withDataFiles(data)
                                .isStreaming(isStreaming)
                                .withBucketPath(pathFactory.bucketPath(part, bucket).toString());

                // 绑定删除向量并返回计划
                if (deletionVectors && beforeDeletionFilesMap != null) {
                    builder.withBeforeDeletionFiles(
                            getDeletionFiles(
                                    before,
                                    beforeDeletionFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyMap())));
                }
                if (deletionVectors && deletionFilesMap != null) {
                    builder.withDataDeletionFiles(
                            getDeletionFiles(
                                    data,
                                    deletionFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyMap())));
                }
                splits.add(builder.build());
            }
        }

        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }
    // Apache Paimon 中用于比较任意两个快照之间差异的核心方法。
    // 它与 readChanges 的不同之处在于：readChanges 通常只比较相邻快照（例如当前与上一个），
    // 而 readIncrementalDiff 允许你传入一个特定的 before 快照，计算从那个时刻到现在的所有变更“差集”。
    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        // 将扫描器切换为 ALL（全量）模式，并获取当前（最新）快照的文件视图。
        withMode(ScanMode.ALL);
        FileStoreScan.Plan plan = scan.plan();
        // 从当前计划中提取出所有类型为 ADD 的文件，并按分区和桶进行分组。
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        // 获取对比快照（Before）的数据文件
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        // 调用 toChangesPlan 核心方法进行“求差集”操作，并封装成最终计划。
        return toChangesPlan(false, plan, new LazyField<>(() -> before), beforeFiles, dataFiles);
    }

    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
        }
        return lazyPartitionComparator;
    }

    private List<DeletionFile> getDeletionFiles(
            List<DataFileMeta> dataFiles, Map<String, DeletionFile> deletionFilesMap) {
        List<DeletionFile> deletionFiles = new ArrayList<>(dataFiles.size());
        dataFiles.stream()
                .map(DataFileMeta::fileName)
                .map(f -> deletionFilesMap == null ? null : deletionFilesMap.get(f))
                .forEach(deletionFiles::add);
        return deletionFiles;
    }

    private Set<Pair<BinaryRow, Integer>> toPartBuckets(
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> entries) {
        return entries.entrySet().stream()
                .flatMap(
                        e ->
                                e.getValue().keySet().stream()
                                        .map(bucket -> Pair.of(e.getKey(), bucket)))
                .collect(Collectors.toSet());
    }
    // Apache Paimon 处理 删除向量（Deletion Vectors, DV） 索引的核心逻辑。
    // 在 Paimon 中，DV 索引文件存储了哪些行已被删除。当读取数据时，必须先找到这些索引文件，以便在读取数据文件时剔除无效行。
    // 为了提高性能，该方法采用了 “两级读取”策略：先查缓存，再读文件系统。
    // 删除向量是对“读时合并（MoR）”的一种极致优化。
    //它不再存储具体的删除行数据，而是存储一个位图（Bitmap）。这个位图对应数据文件中每一行的序列号。
    //原理：位图中的每一个 bit 代表一行。如果第 5 行被删除了，位图中第 5 个位置就标记为 1，否则为 0。
    //存储：这个位图（Roaring Bitmap 格式）非常小，通常存储在专门的索引文件（Index File）中。
    private Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> scanDvIndex(
            @Nullable Snapshot snapshot, Set<Pair<BinaryRow, Integer>> buckets) {
        // 检查快照是否存在索引。
        if (snapshot == null || snapshot.indexManifest() == null) {
            return Collections.emptyMap();
        }
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> result = new HashMap<>();
        // indexManifestPath：获取当前快照对应的索引清单文件路径，它作为缓存的 Key 之一。
        Path indexManifestPath = indexFileHandler.indexManifestFilePath(snapshot.indexManifest());

        // 1. read from cache
        // 第一阶段：从缓存读取 (Read from Cache)
        if (dvMetaCache != null) {
            Iterator<Pair<BinaryRow, Integer>> iterator = buckets.iterator();
            while (iterator.hasNext()) {
                Pair<BinaryRow, Integer> next = iterator.next();
                BinaryRow partition = next.getLeft();
                int bucket = next.getRight();
                Map<String, DeletionFile> fromCache =
                        dvMetaCache.read(indexManifestPath, partition, bucket);
                if (fromCache != null) {
                    result.put(next, fromCache);
                    iterator.remove();
                    if (dvMetaCacheMetrics != null) {
                        dvMetaCacheMetrics.increaseHitObject();
                    }
                } else {
                    if (dvMetaCacheMetrics != null) {
                        dvMetaCacheMetrics.increaseMissedObject();
                    }
                }
            }
        }

        // 2. read from file system
        // 第二阶段：从文件系统扫描 (Read from File System)
        // 调用 indexFileHandler 去物理存储（如 HDFS/S3）扫描索引文件。
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> partitionFileMetas =
                indexFileHandler.scan(
                        snapshot,
                        DELETION_VECTORS_INDEX,
                        buckets.stream().map(Pair::getLeft).collect(Collectors.toSet()));
        // 解析扫描到的物理索引信息，并更新缓存。
        // toDeletionFiles：将底层的索引文件元数据转化为 Paimon 内部易于处理的 DeletionFile 对象（建立数据文件与 DV 文件的对应关系）。
        partitionFileMetas.forEach(
                (entry, indexFileMetas) -> {
                    Map<String, DeletionFile> deletionFiles =
                            toDeletionFiles(entry, indexFileMetas);
                    if (dvMetaCache != null) {
                        dvMetaCache.put(
                                indexManifestPath,
                                entry.getLeft(),
                                entry.getRight(),
                                deletionFiles);
                    }
                    if (buckets.contains(entry)) {
                        result.put(entry, deletionFiles);
                    }
                });
        return result;
    }

    private Map<String, DeletionFile> toDeletionFiles(
            Pair<BinaryRow, Integer> partitionBucket, List<IndexFileMeta> fileMetas) {
        Map<String, DeletionFile> deletionFiles = new HashMap<>();
        DeletionVectorsIndexFile dvIndex =
                indexFileHandler.dvIndex(partitionBucket.getLeft(), partitionBucket.getRight());
        for (IndexFileMeta indexFile : fileMetas) {
            LinkedHashMap<String, DeletionVectorMeta> dvRanges = indexFile.dvRanges();
            String dvFilePath = dvIndex.path(indexFile).toString();
            if (dvRanges != null && !dvRanges.isEmpty()) {
                for (DeletionVectorMeta dvMeta : dvRanges.values()) {
                    deletionFiles.put(
                            dvMeta.dataFileName(),
                            new DeletionFile(
                                    dvFilePath,
                                    dvMeta.offset(),
                                    dvMeta.length(),
                                    dvMeta.cardinality()));
                }
            }
        }
        return deletionFiles;
    }
}

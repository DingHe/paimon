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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ListUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Default implementation of {@link FileStoreScan}. */
// 是 Apache Paimon 中 FileStoreScan 接口的基础实现类。
// 它封装了扫描操作的公共逻辑，特别是如何从 Manifest（清单）文件中高效、并行地提取数据文件条目的核心流程。
// 统一过滤逻辑：它将分区过滤、桶（Bucket）过滤、LSM 层级过滤以及文件名过滤整合在一起，在读取 Manifest 内容时尽可能早地剔除无关文件。
// 并行加速：利用线程池并行读取多个 Manifest 文件。它处理了复杂的并发逻辑（如 readAndMergeFileEntries），
// 确保在 ScanMode.ALL（全量）模式下能正确处理文件的 ADD 和 DELETE 状态，合并出最终的文件视图。
// 解耦存储与逻辑：通过 manifestFileFactory 屏蔽了底层 Manifest 文件的存储细节，专注于扫描业务逻辑。
public abstract class AbstractFileStoreScan implements FileStoreScan {
    // 用于读取 ManifestList 的组件，获取当前快照关联的所有清单文件元数据。
    private final ManifestsReader manifestsReader;
    // 用于通过 ID 获取具体的 Snapshot 对象
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    // 控制读取 Manifest 文件时的并行度
    private final Integer parallelism;
    // 管理表的结构。
    // 由于扫描可能涉及多个快照版本（对应的 Schema ID 可能不同），tableSchemas 使用缓存来保证线程安全地获取不同版本的 Schema。
    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final SchemaManager schemaManager;
    protected final TableSchema schema;
    // 用户指定的具体快照对象
    private Snapshot specifiedSnapshot = null;
    private boolean onlyReadRealBuckets = false;
    // 桶级别的各种过滤策略。
    private Integer specifiedBucket = null;
    private Filter<Integer> bucketFilter = null;
    private BiFilter<Integer, Integer> totalAwareBucketFilter = null;
    // 扫描模式（ALL 全量, DELTA 增量等）
    protected ScanMode scanMode = ScanMode.ALL;
    // LSM 树层级过滤，例如只读 Level 0。
    private Integer specifiedLevel = null;
    private Filter<Integer> levelFilter = null;
    // 针对清单条目或具体文件名的自定义过滤。
    private Filter<ManifestEntry> manifestEntryFilter = null;
    private Filter<String> fileNameFilter = null;
    // 用于记录扫描性能指标（如扫描耗时、读取的文件数等）
    private ScanMetrics scanMetrics = null;
    // 如果为真，则在读取结果中丢弃统计信息（Min/Max），以节省内存
    private boolean dropStats;
    @Nullable protected List<Range> rowRanges;
    @Nullable protected Long limit;

    public AbstractFileStoreScan(
            ManifestsReader manifestsReader,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            @Nullable Integer parallelism) {
        this.manifestsReader = manifestsReader;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.manifestFileFactory = manifestFileFactory;
        this.tableSchemas = new ConcurrentHashMap<>();
        this.parallelism = parallelism;
        this.dropStats = false;
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRow> partitions) {
        manifestsReader.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionsFilter(List<Map<String, String>> partitions) {
        manifestsReader.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(PartitionPredicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public FileStoreScan onlyReadRealBuckets() {
        manifestsReader.onlyReadRealBuckets();
        this.onlyReadRealBuckets = true;
        return this;
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        manifestsReader.withBucket(bucket);
        specifiedBucket = bucket;
        return this;
    }

    @Override
    public FileStoreScan withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withTotalAwareBucketFilter(
            BiFilter<Integer, Integer> totalAwareBucketFilter) {
        this.totalAwareBucketFilter = totalAwareBucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withPartitionBucket(BinaryRow partition, int bucket) {
        withPartitionFilter(Collections.singletonList(partition));
        withBucket(bucket);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        this.specifiedSnapshot = snapshotManager.snapshot(snapshotId);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(Snapshot snapshot) {
        this.specifiedSnapshot = snapshot;
        return this;
    }

    @Override
    public FileStoreScan withKind(ScanMode scanMode) {
        this.scanMode = scanMode;
        return this;
    }

    @Override
    public FileStoreScan withLevel(int level) {
        manifestsReader.withLevel(level);
        this.specifiedLevel = level;
        return this;
    }

    @Override
    public FileStoreScan withLevelFilter(Filter<Integer> levelFilter) {
        this.levelFilter = levelFilter;
        return this;
    }

    @Override
    public FileStoreScan withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        manifestsReader.withLevelMinMaxFilter(minMaxFilter);
        return this;
    }

    @Override
    public FileStoreScan enableValueFilter() {
        return this;
    }

    @Override
    public FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter) {
        this.manifestEntryFilter = filter;
        return this;
    }

    @Override
    public FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter) {
        this.fileNameFilter = fileNameFilter;
        return this;
    }

    @Override
    public FileStoreScan withMetrics(ScanMetrics metrics) {
        this.scanMetrics = metrics;
        return this;
    }

    @Override
    public FileStoreScan dropStats() {
        this.dropStats = true;
        return this;
    }

    @Override
    public FileStoreScan keepStats() {
        this.dropStats = false;
        return this;
    }

    @Override
    public FileStoreScan withRowRanges(List<Range> rowRanges) {
        this.rowRanges = rowRanges;
        return this;
    }

    @Override
    public FileStoreScan withReadType(RowType readType) {
        return this;
    }

    @Override
    public FileStoreScan withLimit(long limit) {
        this.limit = limit;
        return this;
    }

    @Nullable
    @Override
    public Integer parallelism() {
        return parallelism;
    }

    @Override
    public ManifestsReader manifestsReader() {
        return manifestsReader;
    }
    // Apache Paimon 扫描逻辑的核心执行体。
    // 它负责将高层级的查询需求（如“我要读快照 10”）转化为具体的物理文件列表。
    @Override
    public Plan plan() {
        // 记录扫描开始的纳秒时间戳，用于后续计算扫描总耗时。
        long started = System.nanoTime();
        // 调用内部的 manifestsReader 读取 Manifest List。
        // 这一步会确定当前快照涉及哪些清单文件（Manifest Files），并初步应用分区过滤。
        ManifestsReader.Result manifestsResult = readManifests();
        // 从结果中提取当前的 Snapshot 对象和经过初步过滤后的清单文件元数据列表
        Snapshot snapshot = manifestsResult.snapshot;
        List<ManifestFileMeta> manifests = manifestsResult.filteredManifests;
        // 执行清单文件的“后置过滤”。
        // 这是一个钩子方法，允许子类在读取清单条目之前，根据清单文件本身的统计信息（如清单内数据的 Min/Max 范围）进一步剔除不相关的清单文件
        manifests = postFilterManifests(manifests);
        // 并行读取 manifests 列表中的具体条目。false 表示不使用顺序流式读取，而是尽可能并发。
        // 如果是 ALL 模式，此处会自动处理 ADD 和 DELETE 条目的合并逻辑，产出最终有效的文件列表迭代器。
        Iterator<ManifestEntry> iterator = readManifestEntries(manifests, false);
        // Limit 下推优化。如果子类支持将 limit 下推到清单扫描阶段（例如我们只需要前 100 条记录的数据文件），
        // 则在此处对迭代器进行截断，避免读取多余的清单条目。
        if (supportsLimitPushManifestEntries()) {
            iterator = limitPushManifestEntries(iterator);
        }
        // 将迭代器中的清单条目真正加载到内存列表中。
        // 此时，files 包含了所有初步筛选出的物理文件元数据。
        List<ManifestEntry> files = ListUtils.toList(iterator);
        // 条目级后置过滤。如果启用了该开关，会再次对条目列表进行筛选。这通常用于一些无法在读取流中完成的复杂逻辑过滤。
        if (postFilterManifestEntriesEnabled()) {
            files = postFilterManifestEntries(files);
        }

        List<ManifestEntry> result = files;

        long scanDuration = (System.nanoTime() - started) / 1_000_000;
        if (scanMetrics != null) {
            long allDataFiles =
                    manifestsResult.allManifests.stream()
                            .mapToLong(f -> f.numAddedFiles() - f.numDeletedFiles())
                            .sum();
            scanMetrics.reportScan(
                    new ScanStats(
                            scanDuration, // 耗时
                            snapshot == null ? 0 : snapshot.id(), // 快照ID
                            manifests.size(), // 涉及的清单文件数
                            allDataFiles - result.size(), // 过滤掉的文件数
                            result.size())); // 最终选中的文件数
        }

        return new Plan() {
            // 返回快照关联的水位线，用于流式计算的时间属性处理。
            @Nullable
            @Override
            public Long watermark() {
                return snapshot == null ? null : snapshot.watermark();
            }

            @Nullable
            @Override
            public Snapshot snapshot() {
                return snapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return result;
            }
        };
    }

    @Override
    public List<SimpleFileEntry> readSimpleEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Iterator<SimpleFileEntry> iterator =
                scanMode == ScanMode.ALL
                        ? readAndMergeFileEntries(manifests, SimpleFileEntry::from, false)
                        : readAndNoMergeFileEntries(manifests, SimpleFileEntry::from, false);
        List<SimpleFileEntry> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    @Override
    public List<PartitionEntry> readPartitionEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                m -> PartitionEntry.merge(PartitionEntry.merge(readManifest(m)), partitions);
        randomlyOnlyExecute(getExecutorService(parallelism), processor, manifests);
        return partitions.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public List<BucketEntry> readBucketEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Map<Pair<BinaryRow, Integer>, BucketEntry> buckets = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                m -> BucketEntry.merge(BucketEntry.merge(readManifest(m)), buckets);
        randomlyOnlyExecute(getExecutorService(parallelism), processor, manifests);
        return buckets.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator() {
        // useSequential: reduce memory and iterator can be stopping
        return readManifestEntries(readManifests().filteredManifests, true);
    }
    // 主要职责是根据扫描模式决定如何处理清单文件中的 ADD（新增）和 DELETE（删除）标记
    protected Iterator<ManifestEntry> readManifestEntries(
            List<ManifestFileMeta> manifests, boolean useSequential) {
        return scanMode == ScanMode.ALL
                ? readAndMergeFileEntries(manifests, Function.identity(), useSequential)
                : readAndNoMergeFileEntries(manifests, Function.identity(), useSequential);
    }
    // 实现了 Paimon 在 全量扫描（ScanMode.ALL） 模式下的“版本合并”机制：
    // 即如何通过处理 LSM 结构中的 ADD 和 DELETE 标记，还原出当前快照真实的物理文件视图。
    // 该方法遵循 “先找出删除，再读取新增并剔除” 的两阶段处理模式。
    private <T extends FileEntry> Iterator<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests, // 待扫描的清单文件元数据列表。
            Function<List<ManifestEntry>, List<T>> converter, // 转换函数，用于定义将原始 ManifestEntry 转换为什么对象。
            boolean useSequential) { // 开关，决定是“内存节省模式（流式）”还是“性能优先模式（完全并行）”
        // 在所有相关的清单文件中，找出所有被标记为 DELETE 的文件条目。
        Set<Identifier> deletedEntries =
                FileEntry.readDeletedEntries(
                        manifest -> readManifest(manifest, FileEntry.deletedFilter(), null),
                        manifests,
                        parallelism);
        // 如果一个清单文件（ManifestFileMeta）记录的 numAddedFiles 为 0，
        // 说明它里面全是删除记录（已经在第一步处理过了），那么在读取有效数据文件时可以直接跳过这个清单文件。
        manifests =
                manifests.stream()
                        .filter(file -> file.numAddedFiles() > 0)
                        .collect(Collectors.toList());
        // 读取并合并（Filter-on-Read）
        Function<ManifestFileMeta, List<T>> processor =
                manifest ->
                        // 将结果转换为目标类型（如 ManifestEntry 或 SimpleFileEntry）
                        converter.apply(
                                readManifest(
                                        manifest,
                                        // 只读取 kind = ADD 的条目（addFilter）
                                        FileEntry.addFilter(),
                                        // 检查该条目的标识符是否在第一阶段生成的 deletedEntries 黑名单中。
                                        // 如果在，说明该文件已被删除，直接剔除。
                                        entry -> !deletedEntries.contains(entry.identifier())));
        // 顺序批次执行
        if (useSequential) {
            return sequentialBatchedExecute(processor, manifests, parallelism).iterator();
        } else {
            // 随机并行执行
            // 完全并行的乱序执行，但返回结果时会尽可能保持一定的资源回收效率。
            return randomlyExecuteSequentialReturn(processor, manifests, parallelism);
        }
    }
    // 用于 增量扫描（ScanMode.DELTA） 或 变更日志（Changelog）扫描 场景
    // 在这种模式下，Paimon 不需要将 ADD 和 DELETE 条目进行抵消合并，而是原封不动地返回所有的变更记录。
    private <T extends FileEntry> Iterator<T> readAndNoMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<List<ManifestEntry>, List<T>> converter,
            boolean useSequential) {
        Function<ManifestFileMeta, List<T>> reader =
                manifest -> converter.apply(readManifest(manifest));
        if (useSequential) {
            return sequentialBatchedExecute(reader, manifests, parallelism).iterator();
        } else {
            return randomlyExecuteSequentialReturn(reader, manifests, parallelism);
        }
    }

    private ManifestsReader.Result readManifests() {
        return manifestsReader.read(specifiedSnapshot, scanMode);
    }

    // ------------------------------------------------------------------------
    // Start Thread Safe Methods: The following methods need to be thread safe because they will be
    // called by multiple threads
    // ------------------------------------------------------------------------

    /** Note: Keep this thread-safe. */
    protected TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }

    /** Note: Keep this thread-safe. */
    protected abstract boolean filterByStats(ManifestEntry entry);
    // 默认实现，直接返回
    protected List<ManifestFileMeta> postFilterManifests(List<ManifestFileMeta> manifests) {
        return manifests;
    }

    protected boolean postFilterManifestEntriesEnabled() {
        return false;
    }

    protected boolean supportsLimitPushManifestEntries() {
        return false;
    }

    protected Iterator<ManifestEntry> limitPushManifestEntries(Iterator<ManifestEntry> entries) {
        throw new UnsupportedOperationException();
    }

    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        throw new UnsupportedOperationException();
    }

    /** Note: Keep this thread-safe. */
    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return readManifest(manifest, null, null);
    }
    // 实际执行清单文件读取的“物理层”逻辑。
    // 它的作用是将磁盘上的 manifest-xxx 文件转化为内存中的 ManifestEntry 对象列表，并应用极其严格的过滤条件以减少内存消耗。
    private List<ManifestEntry> readManifest(
            ManifestFileMeta manifest,
            @Nullable Filter<InternalRow> additionalFilter,
            @Nullable Filter<ManifestEntry> additionalTFilter) {
        // 通过工厂类创建一个 ManifestFile 读取器实例。它封装了底层文件系统（如 HDFS/S3）的读取逻辑。
        List<ManifestEntry> entries =
                manifestFileFactory
                        .create()
                        .withCacheMetrics(
                                scanMetrics != null ? scanMetrics.getCacheMetrics() : null)
                        .read(
                                // 指定要读取的目标清单文件的名称和字节大小。
                                manifest.fileName(),
                                manifest.fileSize(),
                                // 第一级过滤（分区级）。在读取清单内容时，如果条目所属的分区不符合要求，直接跳过。
                                manifestsReader.partitionFilter(),
                                // 第二级过滤（桶级）。根据用户指定的 bucket 或哈希过滤条件，剔除不相关的桶。
                                createBucketFilter(),
                                // 第三级过滤（行级 - 性能优化）。这是在 InternalRow（原始二进制行）层面进行的过滤。
                                // 它在对象反序列化之前检查分区、桶、层级和文件名。如果不通过，则不会创建 ManifestEntry 对象，极大减少了 GC 压力。
                                createEntryRowFilter().and(additionalFilter),
                                // 第四级过滤（对象级）。此时条目已反序列化为 ManifestEntry 对象
                                entry ->
                                        (additionalTFilter == null || additionalTFilter.test(entry))
                                                && (manifestEntryFilter == null
                                                        || manifestEntryFilter.test(entry))
                                                && filterByStats(entry)); // 最重要的步骤，利用文件内的列统计信息（Min/Max）进行谓词下推（File Skipping）。
        // 如果用户配置了 dropStats()，则遍历所有读取出来的条目，调用 dropStats(entry) 产生一个不含 FieldStats（列统计信息）的副本。
        if (dropStats) {
            List<ManifestEntry> copied = new ArrayList<>(entries.size());
            for (ManifestEntry entry : entries) {
                copied.add(dropStats(entry));
            }
            entries = copied;
        }
        return entries;
    }

    protected ManifestEntry dropStats(ManifestEntry entry) {
        return entry.copyWithoutStats();
    }

    private BucketFilter createBucketFilter() {
        return BucketFilter.create(
                onlyReadRealBuckets, specifiedBucket, bucketFilter, totalAwareBucketFilter);
    }

    /**
     * Read the corresponding entries based on the current required partition and bucket.
     *
     * <p>Implemented to {@link InternalRow} is for performance (No deserialization).
     */
    // createEntryRowFilter 是一个极高性能的过滤组件。
    // 它直接在 InternalRow（内存中的二进制行数据）层面进行判断，而不需要将整行数据反序列化为昂贵的 Java 对象（ManifestEntry）。
    // 只有通过此过滤器的数据，才会被转化为对象，这极大地减少了垃圾回收（GC）压力和 CPU 消耗。
    // 这里过滤的是“清单文件（Manifest File）中的记录”，而这些记录指向的是“数据文件（Data File）”。
    private Filter<InternalRow> createEntryRowFilter() {
        // 在过滤逻辑执行前，代码先准备了一系列“提取器”，
        // 这些提取器知道如何从二进制行中定位特定的字段偏移量。
        // 获取分区字段提取器。用于从原始行中快速读取分区（Partition）信息
        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        // 获取桶（Bucket）编号和总桶数（Total Buckets）的提取器
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        // 获取文件名（File Name）和 LSM 树层级（Level）的提取器
        Function<InternalRow, String> fileNameGetter = ManifestEntrySerializer.fileNameGetter();
        PartitionPredicate partitionFilter = manifestsReader.partitionFilter();
        Function<InternalRow, Integer> levelGetter = ManifestEntrySerializer.levelGetter();
        BucketFilter bucketFilter = createBucketFilter();
        return row -> {
            if ((partitionFilter != null && !partitionFilter.test(partitionGetter.apply(row)))) {
                return false;
            }

            if (bucketFilter != null) {
                int bucket = bucketGetter.apply(row);
                int totalBucket = totalBucketGetter.apply(row);
                if (!bucketFilter.test(bucket, totalBucket)) {
                    return false;
                }
            }

            int level = levelGetter.apply(row);
            if (specifiedLevel != null && level != specifiedLevel) {
                return false;
            }

            if (levelFilter != null && !levelFilter.test(level)) {
                return false;
            }

            return fileNameFilter == null || fileNameFilter.test((fileNameGetter.apply(row)));
        };
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}

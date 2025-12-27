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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FilteredManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.AGGREGATE;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
// KeyValueFileStoreScan 是专门为 有主键表（Primary Key Table） 或 KeyValue 格式表 设计的数据扫描处理器。
// 它继承自 AbstractFileStoreScan，并实现了针对 Key 和 Value 双重过滤的高级逻辑。
// 该类的主要职责是 “在元数据层面进行数据裁剪”。
// 在 LSM 树结构的 KeyValue 存储中，数据不仅按分区和桶拆分，还存在多个层级（Level）。KeyValueFileStoreScan 的作用包括：
// 双重裁剪：同时支持对主键（Key）和值列（Value）的统计信息（Min/Max）进行过滤。
// Schema 演变支持：处理在表结构发生变化（如增加列）后，旧文件与新谓词之间的兼容性转换。
// 桶级优化：在有主键表中，由于相同 Key 的数据可能分布在不同层级的文件中，它负责以“桶”为单位进行整体过滤判断。
// 集成高级索引：利用文件内嵌入的索引（File Index）进一步加速过滤。
public class KeyValueFileStoreScan extends AbstractFileStoreScan {
    // 处理主键列统计信息的演变。当 Schema 改变时，确保旧文件的 Key 统计信息能正确转换。
    private final SimpleStatsEvolutions fieldKeyStatsConverters;
    // 处理值列统计信息的演变。
    private final SimpleStatsEvolutions fieldValueStatsConverters;
    // 将 Key 上的过滤条件转换为对特定桶（Bucket）的过滤，实现桶裁剪。
    private final BucketSelectConverter bucketSelectConverter;
    // 是否启用删除向量（Deletion Vectors）。这会影响合并引擎如何处理数据。
    private final boolean deletionVectorsEnabled;
    // 合并引擎类型（如 DEDUPLICATE, PARTIAL_UPDATE, AGGREGATE）
    private final MergeEngine mergeEngine;
    // Changelog 生成机制
    private final ChangelogProducer changelogProducer;
    // 是否允许读取并利用文件索引进行过滤
    private final boolean fileIndexReadEnabled;
    // 针对主键列的过滤谓词。
    private Predicate keyFilter;
    // 针对值列（非主键列）的过滤谓词
    private Predicate valueFilter;
    // 强制开启 Value 过滤的标记（通常用于全量扫描）
    private boolean valueFilterForceEnabled = false;

    // cache not evolved filter by schema id
    // 缓存不同 Schema ID 下对应的过滤器。避免为清单中的每个文件重复生成过滤器对象。
    private final Map<Long, Predicate> notEvolvedKeyFilterMapping = new ConcurrentHashMap<>();

    // cache not evolved filter by schema id
    private final Map<Long, Predicate> notEvolvedValueFilterMapping = new ConcurrentHashMap<>();

    // cache evolved filter by schema id
    // 专门用于文件索引的过滤器缓存
    private final Map<Long, Predicate> evolvedValueFilterMapping = new ConcurrentHashMap<>();

    public KeyValueFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine,
            ChangelogProducer changelogProducer,
            boolean fileIndexReadEnabled) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism);
        this.bucketSelectConverter = bucketSelectConverter;
        // NOTE: don't add key prefix to field names because fieldKeyStatsConverters is used for
        // filter conversion
        this.fieldKeyStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> scanTableSchema(sid).trimmedPrimaryKeysFields(), schema.id());
        this.fieldValueStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)),
                        schema.id());
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
        this.changelogProducer = changelogProducer;
        this.fileIndexReadEnabled = fileIndexReadEnabled;
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
    }

    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan enableValueFilter() {
        this.valueFilterForceEnabled = true;
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        if (isValueFilterEnabled() && !filterByValueFilter(entry)) {
            return false;
        }

        Predicate notEvolvedFilter =
                notEvolvedKeyFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter to handle add field
                                // for example, add field 'c', 'c > 3': old files can be filtered
                                fieldKeyStatsConverters.filterUnsafeFilter(
                                        entry.file().schemaId(), keyFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result stats =
                fieldKeyStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.keyStats(), file.rowCount(), null);
        return notEvolvedFilter.test(
                file.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    @Override
    protected ManifestEntry dropStats(ManifestEntry entry) {
        if (!isValueFilterEnabled() && postFilterManifestEntriesEnabled()) {
            return new FilteredManifestEntry(entry.copyWithoutStats(), filterByValueFilter(entry));
        }
        return entry.copyWithoutStats();
    }

    private boolean filterByFileIndex(@Nullable byte[] embeddedIndexBytes, ManifestEntry entry) {
        if (embeddedIndexBytes == null) {
            return true;
        }

        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();
        try (FileIndexPredicate predicate =
                new FileIndexPredicate(embeddedIndexBytes, dataRowType)) {
            Predicate dataPredicate =
                    evolvedValueFilterMapping.computeIfAbsent(
                            entry.file().schemaId(),
                            id ->
                                    fieldValueStatsConverters.tryDevolveFilter(
                                            entry.file().schemaId(), valueFilter));
            return predicate.evaluate(dataPredicate).remain();
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking fileIndex predicate.", e);
        }
    }

    private boolean isValueFilterEnabled() {
        if (valueFilter == null) {
            return false;
        }

        switch (scanMode) {
            case ALL:
                return valueFilterForceEnabled;
            case DELTA:
                return false;
            case CHANGELOG:
                return changelogProducer == ChangelogProducer.LOOKUP
                        || changelogProducer == ChangelogProducer.FULL_COMPACTION;
            default:
                throw new UnsupportedOperationException("Unsupported scan mode: " + scanMode);
        }
    }

    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return valueFilter != null && scanMode == ScanMode.ALL;
    }

    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> files) {
        // We group files by bucket here, and filter them by the whole bucket filter.
        // Why do this: because in primary key table, we can't just filter the value
        // by the stat in files (see `PrimaryKeyFileStoreTable.nonPartitionFilterConsumer`),
        // but we can do this by filter the whole bucket files
        return files.stream()
                .collect(
                        Collectors.groupingBy(
                                // we use LinkedHashMap to avoid disorder
                                file -> Pair.of(file.partition(), file.bucket()),
                                LinkedHashMap::new,
                                Collectors.toList()))
                .values()
                .stream()
                .map(this::doFilterWholeBucketByStats)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<ManifestEntry> doFilterWholeBucketByStats(List<ManifestEntry> entries) {
        return noOverlapping(entries)
                ? filterWholeBucketPerFile(entries)
                : filterWholeBucketAllFiles(entries);
    }

    private List<ManifestEntry> filterWholeBucketPerFile(List<ManifestEntry> entries) {
        List<ManifestEntry> filtered = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                filtered.add(entry);
            }
        }
        return filtered;
    }

    private List<ManifestEntry> filterWholeBucketAllFiles(List<ManifestEntry> entries) {
        if (!deletionVectorsEnabled
                && (mergeEngine == PARTIAL_UPDATE || mergeEngine == AGGREGATE)) {
            return entries;
        }

        // entries come from the same bucket, if any of it doesn't meet the request, we could
        // filter the bucket.
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                return entries;
            }
        }
        return Collections.emptyList();
    }

    private boolean filterByValueFilter(ManifestEntry entry) {
        if (entry instanceof FilteredManifestEntry) {
            return ((FilteredManifestEntry) entry).selected();
        }

        Predicate notEvolvedFilter =
                notEvolvedValueFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter to handle add field
                                // for example, add field 'c', 'c > 3': old files can be filtered
                                fieldValueStatsConverters.filterUnsafeFilter(
                                        entry.file().schemaId(), valueFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result result =
                fieldValueStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.valueStats(), file.rowCount(), file.valueStatsCols());
        return notEvolvedFilter.test(
                        file.rowCount(),
                        result.minValues(),
                        result.maxValues(),
                        result.nullCounts())
                && (!fileIndexReadEnabled
                        || filterByFileIndex(entry.file().embeddedIndex(), entry));
    }

    private static boolean noOverlapping(List<ManifestEntry> entries) {
        if (entries.size() <= 1) {
            return true;
        }

        Integer previousLevel = null;
        for (ManifestEntry entry : entries) {
            int level = entry.file().level();
            // level 0 files have overlapping
            if (level == 0) {
                return false;
            }

            if (previousLevel == null) {
                previousLevel = level;
            } else {
                // different level, have overlapping
                if (previousLevel != level) {
                    return false;
                }
            }
        }

        return true;
    }
}

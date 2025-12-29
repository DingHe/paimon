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

package org.apache.paimon;

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.postpone.PostponeBucketFileStoreWrite;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStore} for querying and updating {@link KeyValue}s. */
// KeyValueFileStore 是专门用于处理有主键（Primary Key）表的物理存储引擎实现。
// 它继承自 AbstractFileStore<KeyValue>，是 Paimon 核心的 LSM-Tree（Log-Structured Merge-Tree） 存储逻辑的所在地。
// 核心作用是管理基于键值对（Key-Value）的数据流。
// 实现主键更新逻辑：它负责处理数据的插入、更新和删除，并根据主键进行去重或合并。
// 驱动 LSM 引擎：协调写入（Write-Ahead Log 和 MemTable）、压缩（Compaction）以及读取（Merge-on-Read）过程。
// 支持多种分桶模式：灵活支持固定分桶、动态分桶以及用于特殊场景的推迟分桶模式。
// 元数据与物理存储的桥梁：将逻辑上的主键和值映射为物理文件中的 KeyValue 结构。
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {
    // 标记是否允许跨分区更新主键。
    // 这会直接影响分桶模式（Bucket Mode）的选择。
    private final boolean crossPartitionUpdate;
    // 分桶键（Bucket Key）的行类型。用于计算数据属于哪一个桶。
    private final RowType bucketKeyType;
    // 主键（Key）的行类型。这是 LSM-Tree 排序和去重的核心依据。
    private final RowType keyType;
    // 值（Value）的行类型。包含除主键外的所有数据列。
    private final RowType valueType;
    // 字段提取器。负责从原始行数据中剥离出 Key 和 Value。
    private final KeyValueFieldsExtractor keyValueFieldsExtractor;
    // 主键比较器供应者。提供用于主键排序的 Comparator，确保 LSM 层级文件是有序的。
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    // 日志去重比较器。在生成 Changelog 时，用于判断两行数据内容是否完全一致（可配置忽略某些字段）。
    private final Supplier<RecordEqualiser> logDedupEqualSupplier;
    // 合并函数工厂。
    // 定义了当主键冲突时，如何合并多个 Value（如：去重保留最新、求和、部分更新等）。
    private final MergeFunctionFactory<KeyValue> mfFactory;

    public KeyValueFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            boolean crossPartitionUpdate,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory,
            String tableName,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, schemaManager, schema, tableName, options, partitionType, catalogEnvironment);
        this.crossPartitionUpdate = crossPartitionUpdate;
        this.bucketKeyType = bucketKeyType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyValueFieldsExtractor = keyValueFieldsExtractor;
        this.mfFactory = mfFactory;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
        List<String> logDedupIgnoreFields = options.changelogRowDeduplicateIgnoreFields();
        this.logDedupEqualSupplier =
                options.changelogRowDeduplicate()
                        ? ValueEqualiserSupplier.fromIgnoreFields(valueType, logDedupIgnoreFields)
                        : () -> null;
    }

    @Override
    public BucketMode bucketMode() {
        int bucket = options.bucket();
        switch (bucket) {
            case -2:
                return BucketMode.POSTPONE_MODE;
            case -1:
                return crossPartitionUpdate ? BucketMode.KEY_DYNAMIC : BucketMode.HASH_DYNAMIC;
            default:
                return BucketMode.HASH_FIXED;
        }
    }

    @Override
    public MergeFileSplitRead newRead() {
        return new MergeFileSplitRead(
                options,
                schema,
                keyType,
                valueType,
                newKeyComparator(),
                mfFactory,
                newReaderFactoryBuilder());
    }

    public RawFileSplitRead newBatchRawFileRead() {
        return new RawFileSplitRead(
                fileIO,
                schemaManager,
                schema,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                options.fileIndexReadEnabled(),
                false);
    }

    public KeyValueFileReaderFactory.Builder newReaderFactoryBuilder() {
        return KeyValueFileReaderFactory.builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                FileFormatDiscover.of(options),
                pathFactory(),
                keyValueFieldsExtractor,
                options);
    }

    @Override
    public AbstractFileStoreWrite<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public AbstractFileStoreWrite<KeyValue> newWrite(String commitUser, @Nullable Integer writeId) {
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            return new PostponeBucketFileStoreWrite(
                    fileIO,
                    pathFactory(),
                    schema,
                    commitUser,
                    partitionType,
                    keyType,
                    valueType,
                    mfFactory,
                    this::pathFactory,
                    newReaderFactoryBuilder(),
                    snapshotManager(),
                    newScan(),
                    options,
                    tableName,
                    writeId);
        }
        DynamicBucketIndexMaintainer.Factory indexFactory = null;
        if (bucketMode() == BucketMode.HASH_DYNAMIC) {
            indexFactory = new DynamicBucketIndexMaintainer.Factory(newIndexFileHandler());
        }
        BucketedDvMaintainer.Factory dvMaintainerFactory = null;
        if (options.deletionVectorsEnabled()) {
            dvMaintainerFactory = BucketedDvMaintainer.factory(newIndexFileHandler());
        }
        return new KeyValueFileStoreWrite(
                fileIO,
                schemaManager,
                schema,
                commitUser,
                partitionType,
                keyType,
                valueType,
                keyComparatorSupplier,
                () -> UserDefinedSeqComparator.create(valueType, options),
                logDedupEqualSupplier,
                mfFactory,
                pathFactory(),
                this::pathFactory,
                snapshotManager(),
                newScan(),
                indexFactory,
                dvMaintainerFactory,
                options,
                keyValueFieldsExtractor,
                tableName);
    }

    @Override
    public KeyValueFileStoreScan newScan() {
        BucketMode bucketMode = bucketMode();
        BucketSelectConverter bucketSelectConverter =
                keyFilter -> {
                    if (bucketMode != BucketMode.HASH_FIXED
                            && bucketMode != BucketMode.POSTPONE_MODE) {
                        return Optional.empty();
                    }

                    List<Predicate> bucketFilters =
                            pickTransformFieldMapping(
                                    splitAnd(keyFilter),
                                    keyType.getFieldNames(),
                                    bucketKeyType.getFieldNames());
                    if (!bucketFilters.isEmpty()) {
                        return BucketSelectConverter.create(
                                and(bucketFilters), bucketKeyType, options.bucketFunctionType());
                    }
                    return Optional.empty();
                };

        return new KeyValueFileStoreScan(
                newManifestsReader(),
                bucketSelectConverter,
                snapshotManager(),
                schemaManager,
                schema,
                keyValueFieldsExtractor,
                manifestFileFactory(),
                options.scanManifestParallelism(),
                options.deletionVectorsEnabled(),
                options.mergeEngine(),
                options.changelogProducer(),
                options.fileIndexReadEnabled() && options.deletionVectorsEnabled());
    }

    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return keyComparatorSupplier.get();
    }
}

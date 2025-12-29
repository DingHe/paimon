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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.MergeTreeSplitGenerator;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowKindFilter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreTable} for primary key table. */
// PrimaryKeyFileStoreTable 是处理**主键表（Primary Key Table）**的核心实现类。
// 它继承自 AbstractFileStoreTable，专门负责管理那些具有主键约束、支持按主键更新和删除的数据表。
// 主要作用是将逻辑上的主键表操作转化为底层的 LSM-Tree（Merge Tree）存储引擎操作。
// 数据模型转换：它将用户输入的 InternalRow 转换为内部存储使用的 KeyValue 对象。
// 读时合并（Merge-on-Read）：它定义了如何合并具有相同主键的不同版本数据。
// 支持多种合并引擎：如 Deduplicate（去重）、Partial-update（部分更新）和 Aggregation（聚合）。
public class PrimaryKeyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;
    // 它是真正负责文件 IO、LSM-Tree 管理和版本控制的底层引擎。
    private transient KeyValueFileStore lazyStore;

    @VisibleForTesting
    PrimaryKeyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    public PrimaryKeyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }
    // 初始化底层存储引擎的核心入口。
    // 它采用了**懒加载（Lazy Initialization）**设计，负责将逻辑表结构转换为底层的 LSM-Tree 存储结构。
    @Override
    public KeyValueFileStore store() {
        // 判断底层存储对象是否已经初始化。
        if (lazyStore == null) {
            // 获取表的逻辑行类型（Row Type）
            RowType rowType = tableSchema.logicalRowType();
            // 将表配置（Map 格式）解析为强类型的 CoreOptions 对象
            CoreOptions options = CoreOptions.fromMap(tableSchema.options());
            // 获取主键表的字段提取器
            // 这个提取器专门用于从一行完整数据中区分出哪些字段属于 Key（主键），哪些属于 Value（普通数据列）。
            KeyValueFieldsExtractor extractor =
                    PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;
            // 构造主键的 RowType
            RowType keyType = new RowType(extractor.keyFields(tableSchema));
            // 创建合并函数工厂
            // 主键表的灵魂。它决定了当两条数据主键相同时该如何处理
            MergeFunctionFactory<KeyValue> mfFactory =
                    PrimaryKeyTableUtils.createMergeFunctionFactory(tableSchema, extractor);
            if (options.needLookup()) {
                mfFactory = LookupMergeFunction.wrap(mfFactory, options, keyType, rowType);
            }

            lazyStore =
                    new KeyValueFileStore(
                            fileIO(),
                            schemaManager(),
                            tableSchema,
                            tableSchema.crossPartitionUpdate(),
                            options,
                            tableSchema.logicalPartitionType(),
                            PrimaryKeyTableUtils.addKeyNamePrefix(
                                    tableSchema.logicalBucketKeyType()),
                            keyType,
                            rowType,
                            extractor,
                            mfFactory,
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    @Override
    protected SplitGenerator splitGenerator() {
        CoreOptions options = store().options();
        return new MergeTreeSplitGenerator(
                store().newKeyComparator(),
                options.splitTargetSize(),
                options.splitOpenFileCost(),
                options.deletionVectorsEnabled(),
                options.mergeEngine());
    }

    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadOverwrite();
    }
    // 非常关键的性能优化点。它决定了用户查询中的过滤条件（Predicate）如何下推到文件扫描阶段。
    // 这个函数接收两个参数：scan（负责查找文件的扫描器）和 predicate（用户输入的查询条件）。它的任务是将过滤条件告诉扫描器，以便减少需要读取的文件数量。
    // 案例：假设文件 1 插入了 key=a, value=1，文件 2 更新了 key=a, value=2。
    //风险：如果用户查询 value=1，且我们直接根据这个条件过滤物理文件，那么扫描器会选中文件 1 而跳过文件 2。
    //错误结果：合并后会返回 key=a, value=1，但实际上最新数据是 value=2，正确结果应该是不返回任何数据。
    //结论：在合并之前，不能因为 Value 不匹配就随便丢弃物理文件。
    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> {
            // currently we can only perform filter push down on keys
            // consider this case:
            //   data file 1: insert key = a, value = 1
            //   data file 2: update key = a, value = 2
            //   filter: value = 1
            // if we perform filter push down on values, data file 1 will be chosen, but data
            // file 2 will be ignored, and the final result will be key = a, value = 1 while the
            // correct result is an empty set
            // 从复杂的查询条件中提取出仅针对主键（Primary Key）的过滤条件。
            List<Predicate> keyFilters =
                    pickTransformFieldMapping(
                            splitAnd(predicate),
                            tableSchema.fieldNames(),
                            tableSchema.trimmedPrimaryKeys());
            // 如果存在主键过滤条件，将其下推给扫描器。
            if (!keyFilters.isEmpty()) {
                ((KeyValueFileStoreScan) scan).withKeyFilter(and(keyFilters));
            }

            // support value filter in bucket level
            // 将原始的、包含全量条件的 predicate 作为 ValueFilter 传递给扫描器。
            ((KeyValueFileStoreScan) scan).withValueFilter(predicate);
        };
    }

    @Override
    public InnerTableRead newRead() {
        return new KeyValueTableRead(
                () -> store().newRead(), () -> store().newBatchRawFileRead(), schema());
    }

    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser, @Nullable Integer writeId) {
        KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                rowType(),
                store().newWrite(commitUser, writeId),
                createRowKeyExtractor(),
                (record, rowKind) ->
                        kv.replace(
                                record.primaryKey(),
                                KeyValue.UNKNOWN_SEQUENCE,
                                rowKind,
                                record.row()),
                rowKindGenerator(),
                RowKindFilter.of(coreOptions()));
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        return new LocalTableQuery(this);
    }

    @Override
    @Nullable
    protected Runnable newExpireRunnable() {
        if (coreOptions().bucket() == BucketMode.POSTPONE_BUCKET) {
            return null;
        } else {
            return super.newExpireRunnable();
        }
    }
}

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

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.AppendOnlySplitGenerator;
import org.apache.paimon.table.source.AppendTableRead;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.splitread.AppendTableRawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.DataEvolutionSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RowKindFilter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** {@link FileStoreTable} for append table. */
// 专门用于处理**仅追加表（Append-only Table）**的核心实现类。它继承自 AbstractFileStoreTable，适用于不需要主键约束、只需高效写入和批量读取的场景。
// 主要作用是管理不含主键的数据流。
// 高效写入：由于不需要像主键表那样进行“读时合并”或“写时查找”，它直接将数据追加到文件中，具有极高的写入吞吐量。
// 物理存储简化：数据通常按顺序存储在数据文件中，不涉及 LSM-Tree 的复杂合并逻辑。
// 支持多种分桶模式：支持 BUCKET_UNAWARE（不分桶，纯追加）和固定分桶模式。
// 架构适配：将通用的表操作（Read/Write）桥接到专门的 AppendOnlyFileStore 引擎上。

public class AppendOnlyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;
    // 底层物理存储引擎。
    private transient AppendOnlyFileStore lazyStore;

    AppendOnlyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    public AppendOnlyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    @Override
    public AppendOnlyFileStore store() {
        if (lazyStore == null) {
            lazyStore =
                    new AppendOnlyFileStore(
                            fileIO,
                            schemaManager(),
                            tableSchema,
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalPartitionType(),
                            tableSchema.logicalBucketKeyType(),
                            tableSchema.logicalRowType().notNull(),
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    @Override
    protected SplitGenerator splitGenerator() {
        CoreOptions options = store().options();
        long targetSplitSize = options.splitTargetSize();
        long openFileCost = options.splitOpenFileCost();
        return coreOptions().dataEvolutionEnabled()
                ? new DataEvolutionSplitGenerator(targetSplitSize, openFileCost)
                : new AppendOnlySplitGenerator(targetSplitSize, openFileCost, bucketMode());
    }

    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadAppendOverwrite();
    }

    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> ((AppendOnlyFileStoreScan) scan).withFilter(predicate);
    }

    @Override
    public InnerTableRead newRead() {
        List<Function<SplitReadConfig, SplitReadProvider>> providerFactories = new ArrayList<>();
        if (coreOptions().dataEvolutionEnabled()) {
            // add data evolution first
            providerFactories.add(
                    config ->
                            new DataEvolutionSplitReadProvider(
                                    () -> store().newDataEvolutionRead(), config));
        } else {
            providerFactories.add(
                    config ->
                            new AppendTableRawFileSplitReadProvider(
                                    () -> store().newRead(), config));
        }
        return new AppendTableRead(providerFactories, schema());
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser, @Nullable Integer writeId) {
        BaseAppendFileStoreWrite writer = store().newWrite(commitUser, writeId);
        return new TableWriteImpl<>(
                rowType(),
                writer,
                createRowKeyExtractor(),
                (record, rowKind) -> {
                    Preconditions.checkState(
                            rowKind.isAdd(),
                            "Append only writer can not accept row with RowKind %s",
                            rowKind);
                    return record.row();
                },
                rowKindGenerator(),
                RowKindFilter.of(coreOptions()));
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        throw new UnsupportedOperationException();
    }
}

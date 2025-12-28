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

package org.apache.paimon.io;

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/** Reads {@link InternalRow} from data files. */
// DataFileRecordReader 的核心作用是：将物理文件读取到的原始行数据映射、转换为符合 Table Schema 要求的最终数据行。
// 底层的文件读取器（如 Parquet 或 ORC）只负责从磁盘加载数据，而 DataFileRecordReader 负责在其之上添加 Paimon 特有的逻辑处理，包括：
// 分区填充：将物理路径中的分区值注入到数据行中。
// 列映射与裁剪：处理 Schema 演变（Schema Evolution）导致的列顺序不一致或投影问题。
// 类型转换：处理字段类型的 Cast（强制转换）。
// 系统字段注入：为每一行自动生成或填充 _row_id 和 _sequence_number 等系统元数据。
public class DataFileRecordReader implements FileRecordReader<InternalRow> {
    // 表定义的完整行类型。
    // 用于确定最终输出结果的结构。
    private final RowType tableRowType;
    // 底层的物理读取器（由 Parquet/ORC 格式实现类提供）。
    private final FileRecordReader<InternalRow> reader;
    // 字段索引映射表。
    // 用于处理字段顺序变化。例如，文件里的第 1 列对应表结构的第 3 列。
    @Nullable private final int[] indexMapping;
    // 分区信息。
    // 如果数据行中包含分区字段，该信息用于将静态的分区值（从路径中获取）填充到每一行中。
    @Nullable private final PartitionInfo partitionInfo;
    // 类型转换映射。
    // 当表字段类型发生变更（如 Int 变 Long）时，负责执行转换逻辑。
    @Nullable private final CastFieldGetter[] castMapping;
    // 是否开启行追踪。
    // 如果开启，读取器会动态计算并填入系统字段。
    private final boolean rowTrackingEnabled;
    // 文件中第一行的起始 ID。
    // 用于计算每一行的全局唯一 _row_id
    @Nullable private final Long firstRowId;
    // 当前文件的最大序列号。
    // 通常用于填充系统字段中的 _sequence_number。
    private final long maxSequenceNumber;
    // 系统字段名到行下标的映射，
    // 标明 _row_id 等字段应该放在结果行的哪个位置。
    private final Map<String, Integer> systemFields;
    // 过滤位图。
    // 如果指定了该属性，只有位图中标记为“1”的行号才会被最终返回。
    @Nullable private final RoaringBitmap32 selection;

    public DataFileRecordReader(
            RowType tableRowType,
            FormatReaderFactory readerFactory,
            FormatReaderFactory.Context context,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowTrackingEnabled,
            @Nullable Long firstRowId,
            long maxSequenceNumber,
            Map<String, Integer> systemFields)
            throws IOException {
        this(
                tableRowType,
                createReader(readerFactory, context),
                indexMapping,
                castMapping,
                partitionInfo,
                rowTrackingEnabled,
                firstRowId,
                maxSequenceNumber,
                systemFields,
                context.selection());
    }

    public DataFileRecordReader(
            RowType tableRowType,
            FileRecordReader<InternalRow> reader,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowTrackingEnabled,
            @Nullable Long firstRowId,
            long maxSequenceNumber,
            Map<String, Integer> systemFields,
            @Nullable RoaringBitmap32 selection) {
        this.tableRowType = tableRowType;
        this.reader = reader;
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.firstRowId = firstRowId;
        this.maxSequenceNumber = maxSequenceNumber;
        this.systemFields = systemFields;
        this.selection = selection;
    }

    private static FileRecordReader<InternalRow> createReader(
            FormatReaderFactory readerFactory, FormatReaderFactory.Context context)
            throws IOException {
        try {
            return readerFactory.createReader(context);
        } catch (Exception e) {
            FileUtils.checkExists(context.fileIO(), context.filePath());
            throw e;
        }
    }
    // 作用是**“装饰”**底层的原始迭代器，通过层层包装，将物理文件中的原始数据转换为最终符合业务 Schema、包含分区信息和系统字段的完整行。
    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        // 调用底层的物理读取器（如 ParquetReader）获取一个原始批次。如果返回 null，说明文件已读完。
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        // 向量化/列式处理 (Columnar Optimization)
        // 如果底层支持列式读取（如 ORC/Parquet 向量化读取），直接在向量块上进行批量操作。
        // 这避免了逐行转换的开销，是 Paimon 读取性能优化的关键。
        if (iterator instanceof ColumnarRowIterator) {
            // 注入分区值并应用索引映射（字段重排）
            iterator = ((ColumnarRowIterator) iterator).mapping(partitionInfo, indexMapping);
            if (rowTrackingEnabled) {
                // // 批量分配 row_id 和 sequence_number，列式执行效率极高
                iterator =
                        ((ColumnarRowIterator) iterator)
                                .assignRowTracking(firstRowId, maxSequenceNumber, systemFields);
            }
        } else {
            // 普通行式处理
            // 填充分区信息
            // 物理文件中通常不存储分区列。这里将 partitionInfo（如 dt=2023-10-01）动态注入到每一行对应的位置。
            if (partitionInfo != null) {
                final PartitionSettedRow partitionSettedRow =
                        PartitionSettedRow.from(partitionInfo);
                iterator = iterator.transform(partitionSettedRow::replaceRow);
            }
            // 如果表结构发生了变化（如删减列、调整列顺序），indexMapping 定义了物理列到逻辑列的映射。ProjectedRow 负责按映射关系重组行数据。
            if (indexMapping != null) {
                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
                iterator = iterator.transform(projectedRow::replaceRow);
            }
            // 行追踪与系统字段注入 (Row Tracking)
            if (rowTrackingEnabled && !systemFields.isEmpty()) {
                GenericRow trackingRow = new GenericRow(2);

                int[] fallbackToTrackingMappings = new int[tableRowType.getFieldCount()];
                Arrays.fill(fallbackToTrackingMappings, -1);

                if (systemFields.containsKey(SpecialFields.ROW_ID.name())) {
                    fallbackToTrackingMappings[systemFields.get(SpecialFields.ROW_ID.name())] = 0;
                }
                if (systemFields.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
                    fallbackToTrackingMappings[
                                    systemFields.get(SpecialFields.SEQUENCE_NUMBER.name())] =
                            1;
                }

                FallbackMappingRow fallbackMappingRow =
                        new FallbackMappingRow(fallbackToTrackingMappings);
                final FileRecordIterator<InternalRow> iteratorInner = iterator;
                iterator =
                        iterator.transform(
                                row -> {
                                    if (firstRowId != null) {
                                        trackingRow.setField(
                                                0, iteratorInner.returnedPosition() + firstRowId);
                                    }
                                    trackingRow.setField(1, maxSequenceNumber);
                                    return fallbackMappingRow.replace(row, trackingRow);
                                });
            }
        }
        // 类型转换 (Casting)
        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }
        // 位图过滤 (Selection/Deletion Vector)
        // 应用 RoaringBitmap 过滤器。这通常用于处理 Deletion Vector，即跳过那些在逻辑上已被删除但物理上还存在于文件中的行。
        if (selection != null) {
            iterator = iterator.selection(selection);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

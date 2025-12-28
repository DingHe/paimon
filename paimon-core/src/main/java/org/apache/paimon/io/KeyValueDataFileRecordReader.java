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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link RecordReader} for reading {@link KeyValue} data files. */
// KeyValueDataFileRecordReader 的核心作用是：将物理存储的行数据（InternalRow）还原为 LSM 树操作所需要的键值对对象（KeyValue）。
// 在 Paimon 的底层存储中（如 Parquet 或 ORC 文件），数据是以列式或行式的 InternalRow 格式存储的。但在进行合并（Merge）、压缩（Compaction）或排序（Sort）时，引擎需要处理的是包含元数据（如 Sequence Number、Value Kind、LSM Level）的 KeyValue 对象。
//该类通过包装一个原始的 FileRecordReader，在读取数据批次时，实时地将每一行数据反序列化，并注入该数据所属的 LSM 层级（Level）信息。
public class KeyValueDataFileRecordReader implements FileRecordReader<KeyValue> {
    // 底层的原始物理读取器。
    // 负责从磁盘文件中读取原始的 InternalRow。它实现了 FileRecordReader 接口，意味着它支持按批次读取并能追踪物理位置。
    private final FileRecordReader<InternalRow> reader;
    // KeyValue 对象的序列化与反序列化工具。
    // 负责将读取到的 InternalRow 映射回 KeyValue 对象。
    // 它知道如何解析 Row 中的每一个字段，并将其分配给 KeyValue 对象的 key、value、sequenceNumber 和 valueKind。
    private final KeyValueSerializer serializer;
    // 标识当前读取的文件所属的 LSM 树层级。
    private final int level;

    public KeyValueDataFileRecordReader(
            FileRecordReader<InternalRow> reader, RowType keyType, RowType valueType, int level) {
        this.reader = reader;
        this.serializer = new KeyValueSerializer(keyType, valueType);
        this.level = level;
    }

    @Nullable
    @Override
    public FileRecordIterator<KeyValue> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        return iterator.transform(
                internalRow ->
                        internalRow == null
                                ? null
                                : serializer.fromRow(internalRow).setLevel(level));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.function.Function;

/** A {@link PersistProcessor} to return {@link KeyValue}. */
// 主要职责是在 Lookup 索引中存储和恢复完整的 KeyValue 数据，但不包含物理位置（Position）信息。
// 构建索引阶段：它将 KeyValue 中的值（Value）、序列号（Sequence Number）和行类型（RowKind）打包成一个字节数组。
// 查询索引阶段：它从字节数组中解析并还原出完整的 KeyValue 对象。
// 应用场景：这是 Paimon 最常用的 Lookup 处理器。它适用于普通的 lookup 变更日志生成模式（changelog-producer = lookup），
// 在这种模式下，系统只需要知道旧数据的内容（以便计算 UPDATE_BEFORE），而不需要知道数据在文件中的具体物理行号。
public class PersistValueProcessor implements PersistProcessor<KeyValue> {
    // 专门用于将数据的“值”部分（InternalRow）序列化为字节数组。
    private final Function<InternalRow, byte[]> serializer;
    // 与序列化器对应，将从磁盘读取的字节数组反序列化回 InternalRow
    private final Function<byte[], InternalRow> deserializer;

    public PersistValueProcessor(
            Function<InternalRow, byte[]> serializer, Function<byte[], InternalRow> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }
    // 明确告知 LookupLevels 组件，在扫描数据文件构建索引时，不需要提供物理行号（rowPosition）。
    @Override
    public boolean withPosition() {
        return false;
    }
    // 定义了索引在磁盘上的字节布局
    @Override
    public byte[] persistToDisk(KeyValue kv) {
        byte[] vBytes = serializer.apply(kv.value());
        byte[] bytes = new byte[vBytes.length + 8 + 1];
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.put(0, vBytes);
        // 尾部倒数第 9 字节起: 写入序列号（确保数据的版本顺序）
        segment.putLong(bytes.length - 9, kv.sequenceNumber());
        // 最后一个字节: 写入行类型（标识 INSERT/UPDATE/DELETE）。
        segment.put(bytes.length - 1, kv.valueKind().toByteValue());
        return bytes;
    }

    @Override
    public KeyValue readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        InternalRow value = deserializer.apply(bytes);
        long sequenceNumber = MemorySegment.wrap(bytes).getLong(bytes.length - 9);
        RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
        return new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level);
    }

    public static Factory<KeyValue> factory(RowType valueType) {
        return new Factory<KeyValue>() {
            @Override
            public String identifier() {
                return "value";
            }

            @Override
            public PersistProcessor<KeyValue> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistValueProcessor(
                        serializerFactory.createSerializer(valueType),
                        serializerFactory.createDeserializer(
                                fileSerVersion, valueType, fileSchema));
            }
        };
    }
}

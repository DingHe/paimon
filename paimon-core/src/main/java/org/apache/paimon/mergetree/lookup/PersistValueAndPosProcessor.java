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

// 主要任务是在 Lookup 索引中同时存储数据的值（Value）和物理位置（Position）。
// 全信息集成：它不仅记录了数据在原始文件中的物理位置（用于 Deletion Vectors），还记录了数据完整的值、序列号（Sequence Number）和行类型（RowKind）。
// 支持复杂的 Lookup 场景：它适用于既需要知道数据“是什么”（内容），又需要知道数据“在哪里”（物理位置）的场景。例如，在进行 Partial Update（部分更新）的同时，还需要维护 Deletion Vector 来标记旧数据失效。

/** A {@link PersistProcessor} to return {@link PositionedKeyValue}. */
public class PersistValueAndPosProcessor implements PersistProcessor<PositionedKeyValue> {
    // 一个函数式接口，用于将 InternalRow（数据值部分）转换为字节数组 byte[]。
    // 这通常由 Paimon 的行序列化框架提供。
    private final Function<InternalRow, byte[]> serializer;
    // 一个函数式接口，
    // 用于将字节数组还原回 InternalRow。
    // 它支持 Schema 演进，可以处理旧版本的数据格式。
    private final Function<byte[], InternalRow> deserializer;

    public PersistValueAndPosProcessor(
            Function<InternalRow, byte[]> serializer, Function<byte[], InternalRow> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }
    // 强制要求 LookupLevels 在扫描原始数据文件时提供 rowPosition。
    // 这是因为该处理器需要将物理位置持久化到索引中。
    @Override
    public boolean withPosition() {
        return true;
    }

    @Override
    public byte[] persistToDisk(KeyValue kv) {
        throw new UnsupportedOperationException();
    }
    // 定义了索引数据的物理存储布局
    @Override
    public byte[] persistToDisk(KeyValue kv, long rowPosition) {
        byte[] vBytes = serializer.apply(kv.value());
        // 分配一个长度为 vBytes.length + 8 (rowPosition) + 8 (sequenceNumber) + 1 (valueKind) 的字节数组。
        // 将固定长度的元数据放在末尾，方便读取时快速定位
        byte[] bytes = new byte[vBytes.length + 8 + 8 + 1];
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.put(0, vBytes);
        segment.putLong(bytes.length - 17, rowPosition);
        segment.putLong(bytes.length - 9, kv.sequenceNumber());
        segment.put(bytes.length - 1, kv.valueKind().toByteValue());
        return bytes;
    }

    @Override
    public PositionedKeyValue readFromDisk(
            InternalRow key, int level, byte[] bytes, String fileName) {
        InternalRow value = deserializer.apply(bytes);
        MemorySegment segment = MemorySegment.wrap(bytes);
        long rowPosition = segment.getLong(bytes.length - 17);
        long sequenceNumber = segment.getLong(bytes.length - 9);
        RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
        return new PositionedKeyValue(
                new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level),
                fileName,
                rowPosition);
    }

    public static Factory<PositionedKeyValue> factory(RowType valueType) {
        return new Factory<PositionedKeyValue>() {
            @Override
            public String identifier() {
                return "position-and-value";
            }

            @Override
            public PersistProcessor<PositionedKeyValue> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistValueAndPosProcessor(
                        serializerFactory.createSerializer(valueType),
                        serializerFactory.createDeserializer(
                                fileSerVersion, valueType, fileSchema));
            }
        };
    }
}

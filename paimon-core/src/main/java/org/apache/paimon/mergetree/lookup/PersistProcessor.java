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
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/** Processor to process value. */
// 定义了如何将原始的 KeyValue 数据序列化并持久化到索引文件，以及如何从索引文件中读取并恢复数据。
// PersistProcessor 的作用是解耦索引文件的存储格式与业务逻辑。
// Paimon 的 Lookup 索引（如本地 SST 文件）底层存储的是 byte[]。不同的业务场景对这些 byte[] 的要求不同：
// Deduplicate 场景：索引只需要存“值”本身。
// Partial Update 场景：索引需要存“值”以及相关的元数据。
// Deletion Vector 场景：索引可能需要记录数据在原始文件中的“位置（Position）”。
public interface PersistProcessor<T> {
    // 标识该处理器是否需要记录行号（Row Position）
    // 如果返回 true，则在构建索引时，系统会调用带有 rowPosition 参数的 persistToDisk 方法。
    // 这在需要精确定位数据在原始 DataFile 中位置的场景（如使用删除向量 Deletion Vector 时）非常关键。
    boolean withPosition();
    // 将 KeyValue 记录转换为字节数组以存入索引
    // 这是“写入”阶段的核心。它定义了哪些字段需要被索引，以及如何序列化。例如，它可能只序列化 Value 部分，或者将整个 KeyValue 对象（含 Sequence Number）序列化。
    byte[] persistToDisk(KeyValue kv);
    // 带行号的持久化逻辑。
    default byte[] persistToDisk(KeyValue kv, long rowPosition) {
        throw new UnsupportedOperationException();
    }
    // 将从磁盘读取的字节数组还原为业务对象。
    // 这是“查询”阶段的核心。
    // key: 查询的主键。
    // level: 该数据所在的 LSM-Tree 层级。
    // valueBytes: 从索引文件中查到的原始字节。
    T readFromDisk(InternalRow key, int level, byte[] valueBytes, String fileName);

    /** Factory to create {@link PersistProcessor}. */
    // 创建具体的处理器实例，并提供元数据信息
    interface Factory<T> {
        // 返回该处理器的唯一标识符
        String identifier();

        PersistProcessor<T> create(
                String fileSerVersion, // 索引文件的序列化版本。
                LookupSerializerFactory serializerFactory, // 序列化器工厂，提供具体的序列化实现
                @Nullable RowType fileSchema);
    }
}

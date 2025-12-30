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

import java.util.Arrays;

import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_LONG_SIZE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeLong;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/** A {@link PersistProcessor} to return {@link FilePosition}. */
// 核心任务是在 Lookup 索引中存储并读取数据行在原始文件中的物理位置（Position），而不是存储数据的具体内容。
// 这在 Paimon 的 Deletion Vectors (删除向量) 模式中至关重要：通过 Lookup 索引快速找到某个 Key 所在的行号，从而在删除向量中标记该行已删除。
// 主要作用是实现 Key -> FilePosition 的映射转换
// 构建索引时：它只提取每条记录的 rowPosition（行号），并将其以变长长整型（VarLong）的格式序列化为字节数组。
// 查询索引时：它将读取到的字节还原为行号，并结合文件名封装成 FilePosition 对象。
// 应用场景：主要用于需要通过主键快速定位数据物理位置的操作，典型的就是点更新/点删除下的删除向量维护。

public class PersistPositionProcessor implements PersistProcessor<FilePosition> {

    // 明确告诉 LookupLevels 在构建索引扫描数据文件时，必须提供行号
    @Override
    public boolean withPosition() {
        return true;
    }
    // 原本用于将 KV 对持久化到磁盘
    @Override
    public byte[] persistToDisk(KeyValue kv) {
        throw new UnsupportedOperationException();
    }

    // 执行真正的序列化逻辑
    @Override
    public byte[] persistToDisk(KeyValue kv, long rowPosition) {
        byte[] bytes = new byte[MAX_VAR_LONG_SIZE];
        int len = encodeLong(bytes, rowPosition);
        return Arrays.copyOf(bytes, len);
    }

    // 反序列化过程。
    @Override
    public FilePosition readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        long rowPosition = decodeLong(bytes, 0);
        return new FilePosition(fileName, rowPosition);
    }

    public static Factory<FilePosition> factory() {
        return new Factory<FilePosition>() {
            @Override
            public String identifier() {
                return "position";
            }

            @Override
            public PersistProcessor<FilePosition> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistPositionProcessor();
            }
        };
    }
}

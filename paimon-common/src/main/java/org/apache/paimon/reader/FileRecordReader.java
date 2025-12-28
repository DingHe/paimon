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

package org.apache.paimon.reader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link RecordReader} to support returning {@link FileRecordIterator}. */
// FileRecordReader 是一个专门用于处理物理文件读取的接口。
// 它扩展了基础的 RecordReader 接口，并强制要求返回一个更具体的迭代器类型：FileRecordIterator。
// FileRecordReader 的主要目的是建立物理文件与逻辑数据流之间的桥梁。
// 普通的 RecordReader 可能代表任何数据源（如归并后的流、索引流等），而 FileRecordReader 明确表示其数据来源于磁盘上的文件（如 Parquet、ORC 或 Avro 格式）。
// 它的核心价值在于支持 文件级别的元数据追踪（如获取当前读取的行号）。
// 在 Paimon 的 LSM 树架构中，仅仅读取数据是不够的。为了实现高性能的更新和删除，Paimon 需要维护位置索引（Position Index）或删除向量（Deletion Vectors）。
// 行号追踪：通过返回 FileRecordIterator，Paimon 的 DeletionVectorReader 可以精准地知道文件的第 $n$ 行是否被标记为删除。
public interface FileRecordReader<T> extends RecordReader<T> {
    // 读取文件中的下一批数据。
    @Override
    @Nullable
    FileRecordIterator<T> readBatch() throws IOException;
}

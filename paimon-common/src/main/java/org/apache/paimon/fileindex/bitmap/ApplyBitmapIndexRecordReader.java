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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link RecordReader} which apply {@link BitmapIndexResult} to filter record. */
// ApplyBitmapIndexRecordReader 的主要作用是：利用预先计算好的位图索引结果，对物理文件读取的数据批次进行精确的行级过滤。
// 在 Paimon 中，位图索引可以记录某个值在文件中的具体行号。当查询条件匹配索引时，会生成一个 BitmapIndexResult。
// 该类负责在读取数据文件时，将这个索引结果应用到数据流上，从而直接跳过那些不符合查询条件的行。
// 这不仅减少了发往上层计算引擎的数据量，也避免了 CPU 对不匹配数据的无效解压和处理。
public class ApplyBitmapIndexRecordReader implements FileRecordReader<InternalRow> {
    // 被包装的原始读取器（通常是 DataFileRecordReader）
    // 负责从磁盘物理文件中实际读取数据。由于它实现了 FileRecordReader 接口，
    // 因此它能够提供数据在文件中的物理行号（Position），这是位图过滤的前提。
    private final FileRecordReader<InternalRow> reader;
    // 图索引的计算结果。
    // 内部通常封装了一个位图（如 RoaringBitmap），标记了哪些行号（Row ID）是符合过滤条件的“有效行”。
    private final BitmapIndexResult fileIndexResult;

    public ApplyBitmapIndexRecordReader(
            FileRecordReader<InternalRow> reader, BitmapIndexResult fileIndexResult) {
        this.reader = reader;
        this.fileIndexResult = fileIndexResult;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> batch = reader.readBatch();
        if (batch == null) {
            return null;
        }

        return new ApplyBitmapIndexFileRecordIterator(batch, fileIndexResult);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

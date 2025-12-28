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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link RecordReader} which apply {@link DeletionVector} to filter record. */
// ApplyDeletionVectorReader 是一个装饰器读取器，其核心作用是：利用删除向量（Deletion Vector，简称 DV）对物理文件读取的数据进行实时过滤。
// 在 Paimon 中，当一条数据被删除或更新时，为了避免重写整个大文件，系统会将该记录在文件中的“行号”记录在一个外部的 DV 文件中。 读取时，该类将原始读取器与对应的 DV 结合：
//从磁盘读取原始数据。
//检查每行数据的行号是否存在于 DV 中。
//如果行号被标记为已删除，则直接跳过，不返回给上层计算引擎。
public class ApplyDeletionVectorReader implements FileRecordReader<InternalRow> {

    private final FileRecordReader<InternalRow> reader;

    private final DeletionVector deletionVector;

    public ApplyDeletionVectorReader(
            FileRecordReader<InternalRow> reader, DeletionVector deletionVector) {
        this.reader = reader;
        this.deletionVector = deletionVector;
    }

    public RecordReader<InternalRow> reader() {
        return reader;
    }

    public DeletionVector deletionVector() {
        return deletionVector;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        return new ApplyDeletionFileRecordIterator(batch, deletionVector);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/**
 * Entry of a manifest file, representing an addition / deletion of a data file.
 *
 * @since 0.9.0
 */
// 是 Manifest File（清单文件） 内部存储的真实数据单元
// ManifestEntry 代表了对一个 数据文件（Data File） 的一次操作记录。
// 状态记录器：它不仅仅记录“有哪些文件”，还记录了这些文件是“新增加的（ADD）”还是“被标记删除的（DELETE）”。
// 连接纽带：它将文件的物理信息（文件名、大小、记录数）与逻辑信息（分区、桶、LSM 级数）绑定在一起。
// 元数据存储实体：它是真正会被序列化并写入磁盘（.manifest 文件）的对象。通过这些条目，Paimon 能够构建出表在任何快照下的文件视图。
@Public
public interface ManifestEntry extends FileEntry {
    // 定义了该条目在持久化（序列化为 Avro 或 ORC）时的物理结构。
    RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            // 对应 FileKind，存为 TinyInt（0 或 1）
                            new DataField(0, "_KIND", new TinyIntType(false)),
                            // _PARTITION: 分区信息，存为二进制。
                            new DataField(1, "_PARTITION", newBytesType(false)),
                            new DataField(2, "_BUCKET", new IntType(false)),
                            new DataField(3, "_TOTAL_BUCKETS", new IntType(false)),
                            new DataField(4, "_FILE", DataFileMeta.SCHEMA)));

    static ManifestEntry create(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        return new PojoManifestEntry(kind, partition, bucket, totalBuckets, file);
    }

    DataFileMeta file();

    ManifestEntry copyWithoutStats();

    ManifestEntry assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber);

    ManifestEntry assignFirstRowId(long firstRowId);

    ManifestEntry upgrade(int newLevel);

    static long recordCount(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream().mapToLong(manifest -> manifest.file().rowCount()).sum();
    }

    static long recordCountAdd(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.ADD.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }

    static long recordCountDelete(List<ManifestEntry> manifestEntries) {
        return manifestEntries.stream()
                .filter(manifestEntry -> FileKind.DELETE.equals(manifestEntry.kind()))
                .mapToLong(manifest -> manifest.file().rowCount())
                .sum();
    }
}

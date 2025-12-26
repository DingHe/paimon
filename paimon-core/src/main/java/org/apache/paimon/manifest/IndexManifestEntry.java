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
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * Manifest entry for index file.
 *
 * @since 0.9.0
 */
// IndexManifestEntry 是索引清单文件的基本数据单元。
// 如果说 ManifestEntry 记录的是数据文件的变更，那么 IndexManifestEntry 记录的就是**索引文件（Index File）**的变更。
// 索引版本追踪：Paimon 的索引（如 Hash Index 或 Deletion Vectors）也是随快照（Snapshot）增量更新的。这个类标记了某个索引文件是被“添加”还是“删除”。
// 物理与逻辑的映射：它将物理上的索引文件（文件名、大小等）与逻辑上的数据归属（属于哪个分区、哪个桶）关联起来。
// 元数据持久化：它是写入 index-manifest-x 文件的真实行数据。当 Paimon 需要定位某个桶的删除向量（Deletion Vector）或索引信息时，会通过扫描这些 Entry 来查找到具体的物理文件。
@Public
public class IndexManifestEntry {

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_KIND", new TinyIntType(false)),
                            new DataField(1, "_PARTITION", newBytesType(false)),
                            new DataField(2, "_BUCKET", new IntType(false)),
                            new DataField(3, "_INDEX_TYPE", newStringType(false)),
                            new DataField(4, "_FILE_NAME", newStringType(false)),
                            new DataField(5, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(6, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(
                                    7,
                                    "_DELETIONS_VECTORS_RANGES",
                                    new ArrayType(true, DeletionVectorMeta.SCHEMA)),
                            new DataField(8, "_EXTERNAL_PATH", newStringType(true)),
                            new DataField(9, "_GLOBAL_INDEX", GlobalIndexMeta.SCHEMA)));

    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final IndexFileMeta indexFile;

    public IndexManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, IndexFileMeta indexFile) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.indexFile = indexFile;
    }

    public IndexManifestEntry toDeleteEntry() {
        checkArgument(kind == FileKind.ADD);
        return new IndexManifestEntry(FileKind.DELETE, partition, bucket, indexFile);
    }

    public FileKind kind() {
        return kind;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public IndexFileMeta indexFile() {
        return indexFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexManifestEntry entry = (IndexManifestEntry) o;
        return bucket == entry.bucket
                && kind == entry.kind
                && Objects.equals(partition, entry.partition)
                && Objects.equals(indexFile, entry.indexFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, indexFile);
    }

    @Override
    public String toString() {
        return "IndexManifestEntry{"
                + "kind="
                + kind
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", indexFile="
                + indexFile
                + '}';
    }
}

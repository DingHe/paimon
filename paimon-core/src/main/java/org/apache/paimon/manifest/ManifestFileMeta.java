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
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Metadata of a manifest file.
 *
 * @since 0.9.0
 */
// 我们可以把 Paimon 的元数据结构看作一个树状层级： Snapshot -> Manifest List -> Manifest File -> Data File。
// ManifestFileMeta 的作用就是在 Manifest List 中代表一个具体的 Manifest File。它的核心价值在于查询裁剪（Pruning）：
// 过滤加速：它记录了该清单文件所包含的所有数据文件的统计信息（如分区范围、Bucket 范围）。
// 元数据索引：在扫描数据之前，Paimon 会先读取 Manifest List 里的这些 ManifestFileMeta，如果某个清单文件的分区范围与查询条件不匹配，则直接跳过该清单文件及其下属的所有数据文件，从而极大提高查询效率。
@Public
public class ManifestFileMeta {
    // SCHEMA: 定义了该类在序列化时的结构（RowType）。
    // 它指定了字段的顺序和类型（如 _FILE_NAME 是 VarChar，_FILE_SIZE 是 BigInt），这使得 Paimon 可以将这些元数据像普通表数据一样高效地存储。
    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            // 清单文件的名称（通常是随机生成的 UUID 文件名）
                            new DataField(
                                    0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            // 该清单中标记为“新增（ADD）”状态的数据文件数量。
                            new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)),
                            // 该清单中标记为“删除（DELETE）”状态的数据文件数量。
                            new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)),
                            // 最核心属性。类型为 SimpleStats，
                            // 记录了该清单中所有数据文件所属分区的最小值和最大值。通过它，引擎能快速判断该清单是否包含目标分区的数据。
                            new DataField(4, "_PARTITION_STATS", SimpleStats.SCHEMA),
                            // 写入该清单文件时所使用的表结构（Schema）ID
                            new DataField(5, "_SCHEMA_ID", new BigIntType(false)),
                            // 该清单包含的数据文件所属 Bucket（桶）的范围。
                            new DataField(6, "_MIN_BUCKET", new IntType(true)),
                            new DataField(7, "_MAX_BUCKET", new IntType(true)),
                            // 该清单包含的数据文件在 LSM 树中的层级（Level）范围
                            new DataField(8, "_MIN_LEVEL", new IntType(true)),
                            new DataField(9, "_MAX_LEVEL", new IntType(true)),
                            // 该清单中包含的数据行的唯一 ID 范围（如果开启了 Row ID 追踪）。
                            new DataField(10, "_MIN_ROW_ID", new BigIntType(true)),
                            new DataField(11, "_MAX_ROW_ID", new BigIntType(true))));

    private final String fileName;
    private final long fileSize;
    private final long numAddedFiles;
    private final long numDeletedFiles;
    private final SimpleStats partitionStats;
    private final long schemaId;
    private final @Nullable Integer minBucket;
    private final @Nullable Integer maxBucket;
    private final @Nullable Integer minLevel;
    private final @Nullable Integer maxLevel;
    private final @Nullable Long minRowId;
    private final @Nullable Long maxRowId;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            SimpleStats partitionStats,
            long schemaId,
            @Nullable Integer minBucket,
            @Nullable Integer maxBucket,
            @Nullable Integer minLevel,
            @Nullable Integer maxLevel,
            @Nullable Long minRowId,
            @Nullable Long maxRowId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
        this.minBucket = minBucket;
        this.maxBucket = maxBucket;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.minRowId = minRowId;
        this.maxRowId = maxRowId;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long numAddedFiles() {
        return numAddedFiles;
    }

    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    public SimpleStats partitionStats() {
        return partitionStats;
    }

    public long schemaId() {
        return schemaId;
    }

    public @Nullable Integer minBucket() {
        return minBucket;
    }

    public @Nullable Integer maxBucket() {
        return maxBucket;
    }

    public @Nullable Integer minLevel() {
        return minLevel;
    }

    public @Nullable Integer maxLevel() {
        return maxLevel;
    }

    public @Nullable Long minRowId() {
        return minRowId;
    }

    public @Nullable Long maxRowId() {
        return maxRowId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId
                && Objects.equals(minBucket, that.minBucket)
                && Objects.equals(maxBucket, that.maxBucket)
                && Objects.equals(minLevel, that.minLevel)
                && Objects.equals(maxLevel, that.maxLevel)
                && Objects.equals(minRowId, that.minRowId)
                && Objects.equals(maxRowId, that.maxRowId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                partitionStats,
                schemaId,
                minBucket,
                maxBucket,
                minLevel,
                maxLevel,
                minRowId,
                maxRowId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d, %s, %s, %s, %s, %s, %s}",
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                partitionStats,
                schemaId,
                minBucket,
                maxBucket,
                minLevel,
                maxLevel,
                minRowId,
                maxRowId);
    }

    // ----------------------- Serialization -----------------------------

    private static final ThreadLocal<ManifestFileMetaSerializer> SERIALIZER_THREAD_LOCAL =
            ThreadLocal.withInitial(ManifestFileMetaSerializer::new);

    public byte[] toBytes() throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().serializeToBytes(this);
    }

    public ManifestFileMeta fromBytes(byte[] bytes) throws IOException {
        return SERIALIZER_THREAD_LOCAL.get().deserializeFromBytes(bytes);
    }
}

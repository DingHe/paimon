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

import org.apache.paimon.data.BinaryRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** A simple {@link FileEntry} only contains identifier and min max key. */
// SimpleFileEntry 的主要作用是 “轻量级的文件元数据载体”。
// 在 Paimon 扫描清单（Manifest）时，标准的 ManifestEntry 包含了非常详细的列统计信息（如每一列的 Min/Max/Null Count）。虽然这些信息对过滤很有用，但它们会占用大量的内存。
// SimpleFileEntry 剥离了那些繁重的列统计信息，仅保留了：
//标识符信息：用于唯一确定一个文件（分区、桶、层级、文件名）。
//核心范围信息：主键的最小值（minKey）和最大值（maxKey）。
// 使用场景：
//
//当系统只需要进行文件级别的合并（Add 与 Delete 抵消）而不需要根据列统计信息进行谓词下推过滤时。
//
//在内存受限的情况下读取大量清单条目。
//
//用于构建文件的索引结构或进行简单的范围重叠判断。
public class SimpleFileEntry implements FileEntry {
    // 文件的动作类型，
    // 分为 ADD（新增）或 DELETE（删除）。在 LSM 结构中，通过合并相同标识符的 ADD 和 DELETE 条目来确定文件的存在性。
    private final FileKind kind;
    // 该文件所属的分区信息
    private final BinaryRow partition;
    // 文件所在的桶（Bucket）编号
    private final int bucket;
    // 写入该文件时表的总桶数（用于处理动态桶伸缩）
    private final int totalBuckets;
    // 该文件所在的 LSM 树层级
    private final int level;
    // 数据文件的名称（例如 data-xxx.orc 或 .parquet）
    private final String fileName;
    // 与该数据文件关联的辅助文件（如外部索引文件）
    private final List<String> extraFiles;
    // 嵌入在清单中的索引数据（可选），用于加速点查
    @Nullable private final byte[] embeddedIndex;
    // 该文件中包含的所有记录中，主键的最小值
    private final BinaryRow minKey;
    // 该文件中包含的所有记录中，主键的最大值
    private final BinaryRow maxKey;
    // 如果文件存储在表路径之外（外部表），则记录其绝对路径
    @Nullable private final String externalPath;

    public SimpleFileEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            int level,
            String fileName,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            BinaryRow minKey,
            BinaryRow maxKey,
            @Nullable String externalPath) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.level = level;
        this.fileName = fileName;
        this.extraFiles = extraFiles;
        this.embeddedIndex = embeddedIndex;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.externalPath = externalPath;
    }

    public static SimpleFileEntry from(ManifestEntry entry) {
        return new SimpleFileEntry(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                entry.level(),
                entry.fileName(),
                entry.file().extraFiles(),
                entry.file().embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.externalPath());
    }

    public SimpleFileEntry toDelete() {
        return new SimpleFileEntry(
                FileKind.DELETE,
                partition,
                bucket,
                totalBuckets,
                level,
                fileName,
                extraFiles,
                embeddedIndex,
                minKey,
                maxKey,
                externalPath);
    }

    public static List<SimpleFileEntry> from(List<ManifestEntry> entries) {
        return entries.stream().map(SimpleFileEntry::from).collect(Collectors.toList());
    }

    @Override
    public FileKind kind() {
        return kind;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public int totalBuckets() {
        return totalBuckets;
    }

    @Override
    public int level() {
        return level;
    }

    @Override
    public String fileName() {
        return fileName;
    }

    @Nullable
    public byte[] embeddedIndex() {
        return embeddedIndex;
    }

    @Nullable
    @Override
    public String externalPath() {
        return externalPath;
    }

    @Override
    public Identifier identifier() {
        return new Identifier(
                partition, bucket, level, fileName, extraFiles, embeddedIndex, externalPath);
    }

    @Override
    public BinaryRow minKey() {
        return minKey;
    }

    @Override
    public BinaryRow maxKey() {
        return maxKey;
    }

    @Override
    public List<String> extraFiles() {
        return extraFiles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleFileEntry that = (SimpleFileEntry) o;
        return bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && level == that.level
                && kind == that.kind
                && Objects.equals(partition, that.partition)
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(extraFiles, that.extraFiles)
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey)
                && Objects.equals(externalPath, that.externalPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                kind,
                partition,
                bucket,
                totalBuckets,
                level,
                fileName,
                extraFiles,
                minKey,
                maxKey,
                externalPath);
    }

    @Override
    public String toString() {
        return "{"
                + "kind="
                + kind
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", totalBuckets="
                + totalBuckets
                + ", level="
                + level
                + ", fileName="
                + fileName
                + ", extraFiles="
                + extraFiles
                + ", minKey="
                + minKey
                + ", maxKey="
                + maxKey
                + ", externalPath="
                + externalPath
                + '}';
    }
}

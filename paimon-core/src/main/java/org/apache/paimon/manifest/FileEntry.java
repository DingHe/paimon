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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Entry representing a file. */
// FileEntry 代表了存储层中一个具体文件的“状态条目”。
// 统一抽象：Paimon 的 Manifest 文件中记录了大量的数据文件（Data File）和更新日志文件（Changelog File）。FileEntry 提供了访问这些文件元数据的统一接口。
// 版本演进逻辑：该接口通过静态方法（如 mergeEntries）实现了 Paimon 的核心元数据合并逻辑——即如何根据一系列“添加（ADD）”和“删除（DELETE）”条目计算出当前时刻真正有效的文件集合。
// 并发读取优化：内置了支持并行读取 Manifest 文件的工具方法，显著提升了在超大规模元数据场景下的 Scan（扫描）性能。
public interface FileEntry {
    // 返回文件操作类型（ADD 或 DELETE）
    FileKind kind();
    // 获取该文件所属的分区（BinaryRow 格式）
    BinaryRow partition();
    // 获取该文件所在的桶 ID。
    int bucket();
    // 获取该表定义的总桶数。
    int totalBuckets();
    // 获取该文件在 LSM 树中的层级（Level 0 到 Level N）
    int level();
    // 获取文件的名称。
    String fileName();
    // 获取文件的外部存储路径（如果有）
    @Nullable
    String externalPath();
    // 生成并返回该文件的唯一标识符 Identifier
    Identifier identifier();
    // 获取该文件中包含的主键最小值和最大值（用于主键表裁剪）。
    BinaryRow minKey();

    BinaryRow maxKey();
    // 获取该文件关联的其他文件列表。
    List<String> extraFiles();

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    // 文件的唯一逻辑标识符
    // 在合并元数据时，如果两个条目的 Identifier 相同，说明它们指向同一个物理文件。
    class Identifier {
        // 文件的分区信息
        public final BinaryRow partition;
        // 所在的桶
        public final int bucket;
        // 在 LSM 树中的层级
        public final int level;
        // 文件的真实名称
        public final String fileName;
        // 关联的辅助文件（如索引文件）
        public final List<String> extraFiles;
        // 嵌入式索引数据
        @Nullable public final byte[] embeddedIndex;
        // 外部路径（用于引用表）
        @Nullable public final String externalPath;

        /* Cache the hash code for the string */
        private Integer hash;

        public Identifier(
                BinaryRow partition,
                int bucket,
                int level,
                String fileName,
                List<String> extraFiles,
                @Nullable byte[] embeddedIndex,
                @Nullable String externalPath) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
            this.extraFiles = extraFiles;
            this.embeddedIndex = embeddedIndex;
            this.externalPath = externalPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Identifier that = (Identifier) o;
            return bucket == that.bucket
                    && level == that.level
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(fileName, that.fileName)
                    && Objects.equals(extraFiles, that.extraFiles)
                    && Objects.deepEquals(embeddedIndex, that.embeddedIndex)
                    && Objects.deepEquals(externalPath, that.externalPath);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash =
                        Objects.hash(
                                partition,
                                bucket,
                                level,
                                fileName,
                                extraFiles,
                                Arrays.hashCode(embeddedIndex),
                                externalPath);
            }
            return hash;
        }

        @Override
        public String toString() {
            return "{partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", level="
                    + level
                    + ", fileName="
                    + fileName
                    + ", extraFiles="
                    + extraFiles
                    + ", embeddedIndex="
                    + Arrays.toString(embeddedIndex)
                    + ", externalPath="
                    + externalPath
                    + '}';
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName
                    + ", extraFiles "
                    + extraFiles
                    + ", embeddedIndex "
                    + Arrays.toString(embeddedIndex)
                    + ", externalPath "
                    + externalPath;
        }
    }
    // 遍历一组条目，使用 Identifier 作为 Key。
    // 如果遇到 ADD：将文件加入结果集。
    // 如果遇到 DELETE：查看结果集中是否有对应的 ADD。如果有，则两者抵消（说明文件在该版本中产生又被删除了）；如果没有，则保留 DELETE（说明是在删除之前版本的文件）。
    static <T extends FileEntry> Collection<T> mergeEntries(Iterable<T> entries) {
        LinkedHashMap<Identifier, T> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    static void mergeEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            Map<Identifier, ManifestEntry> map,
            @Nullable Integer manifestReadParallelism) {
        mergeEntries(
                readManifestEntries(manifestFile, manifestFiles, manifestReadParallelism), map);
    }

    static <T extends FileEntry> void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
        for (T entry : entries) {
            Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added.",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // each dataFile will only be added once and deleted once,
                    // if we know that it is added before then both add and delete entry can be
                    // removed because there won't be further operations on this file,
                    // otherwise we have to keep the delete entry because the add entry must be
                    // in the previous manifest files
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    static Iterable<ManifestEntry> readManifestEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        return sequentialBatchedExecute(
                file -> manifestFile.read(file.fileName(), file.fileSize()),
                manifestFiles,
                manifestReadParallelism);
    }

    static Set<Identifier> readDeletedEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        return readDeletedEntries(
                m ->
                        manifestFile.read(
                                m.fileName(), m.fileSize(), deletedFilter(), Filter.alwaysTrue()),
                manifestFiles,
                manifestReadParallelism);
    }

    static <T extends FileEntry> Set<Identifier> readDeletedEntries(
            Function<ManifestFileMeta, List<T>> manifestReader,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        manifestFiles =
                manifestFiles.stream()
                        .filter(file -> file.numDeletedFiles() > 0)
                        .collect(Collectors.toList());
        Function<ManifestFileMeta, List<Identifier>> processor =
                file ->
                        manifestReader.apply(file).stream()
                                // filter again, ensure is delete
                                .filter(e -> e.kind() == FileKind.DELETE)
                                .map(FileEntry::identifier)
                                .collect(Collectors.toList());
        Iterator<Identifier> identifiers =
                randomlyExecuteSequentialReturn(processor, manifestFiles, manifestReadParallelism);
        Set<Identifier> result = ConcurrentHashMap.newKeySet();
        while (identifiers.hasNext()) {
            result.add(identifiers.next());
        }
        return result;
    }

    static Filter<InternalRow> deletedFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.DELETE;
    }

    static Filter<InternalRow> addFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.ADD;
    }
}

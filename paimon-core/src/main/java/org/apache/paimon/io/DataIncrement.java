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

package org.apache.paimon.io;

import org.apache.paimon.index.IndexFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Increment of data files, changelog files and index files. */
// DataIncrement 是一个非常关键的容器类（POJO），它封装了一个写入事务中产生的所有文件变更信息。
// DataIncrement（数据增量）的主要作用是作为元数据载体，描述了表在某个批次写入（例如 Flink 的一个 Checkpoint）后，底层存储文件的具体变化情况。
// Paimon 基于 LSM-Tree 架构，数据的变更并不是直接修改原文件，而是通过生成新文件和标记旧文件删除来实现的。DataIncrement 就像是一个“变动清单”，它告诉系统：
// 增加了哪些新数据文件？
// 哪些旧文件因为合并（Compaction）而失效了？
// 产生了哪些用于流式消费的 Changelog 文件？
// 索引文件（Lookup 索引等）有什么变化？

public class DataIncrement {
    // 新增数据文件列表。
    // 包含本次写入产生的 L0 文件，或 Compaction 产生的高层文件。
    private final List<DataFileMeta> newFiles;
    // 待删除数据文件列表。记录因合并而被替换掉的旧文件。
    // 注意：在 Paimon 中，“删除”通常是逻辑标记，文件物理删除由清理机制负责。
    private final List<DataFileMeta> deletedFiles;
    // 变更日志文件列表。如果启用了 changelog-producer，这些文件记录了 INSERT/UPDATE/DELETE 的具体流水。
    private final List<DataFileMeta> changelogFiles;
    // 新增索引文件列表。例如 Hash 索引或 Lookup 索引在写入后产生的新索引文件。
    private final List<IndexFileMeta> newIndexFiles;
    // 待删除索引文件列表。过时的索引文件，逻辑同 deletedFiles。
    private final List<IndexFileMeta> deletedIndexFiles;

    public DataIncrement(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> changelogFiles) {
        this(newFiles, deletedFiles, changelogFiles, new ArrayList<>(), new ArrayList<>());
    }

    public DataIncrement(
            List<DataFileMeta> newFiles,
            List<DataFileMeta> deletedFiles,
            List<DataFileMeta> changelogFiles,
            List<IndexFileMeta> newIndexFiles,
            List<IndexFileMeta> deletedIndexFiles) {
        this.newFiles = newFiles;
        this.deletedFiles = deletedFiles;
        this.changelogFiles = changelogFiles;
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = deletedIndexFiles;
    }

    public static DataIncrement emptyIncrement() {
        return new DataIncrement(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    public static DataIncrement indexIncrement(List<IndexFileMeta> indexFiles) {
        return new DataIncrement(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                indexFiles,
                Collections.emptyList());
    }

    public List<DataFileMeta> newFiles() {
        return newFiles;
    }

    public List<DataFileMeta> deletedFiles() {
        return deletedFiles;
    }

    public List<DataFileMeta> changelogFiles() {
        return changelogFiles;
    }

    public List<IndexFileMeta> newIndexFiles() {
        return newIndexFiles;
    }

    public List<IndexFileMeta> deletedIndexFiles() {
        return deletedIndexFiles;
    }

    public boolean isEmpty() {
        return newFiles.isEmpty()
                && deletedFiles.isEmpty()
                && changelogFiles.isEmpty()
                && newIndexFiles.isEmpty()
                && deletedIndexFiles.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataIncrement that = (DataIncrement) o;
        return Objects.equals(newFiles, that.newFiles)
                && Objects.equals(deletedFiles, that.deletedFiles)
                && Objects.equals(changelogFiles, that.changelogFiles)
                && Objects.equals(newIndexFiles, that.newIndexFiles)
                && Objects.equals(deletedIndexFiles, that.deletedIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                newFiles, deletedFiles, changelogFiles, newIndexFiles, deletedIndexFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "DataIncrement {newFiles = %s, deletedFiles = %s, changelogFiles = %s, newIndexFiles = %s, deletedIndexFiles = %s}",
                newFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                deletedFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                changelogFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                newIndexFiles.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()),
                deletedIndexFiles.stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList()));
    }
}

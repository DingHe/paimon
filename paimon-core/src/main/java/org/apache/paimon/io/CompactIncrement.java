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

/** Files changed before and after compaction, with changelog produced during compaction. */
// CompactIncrement 是一个关键的元数据容器类。它与您之前看到的 DataIncrement 类似，但它专门用于描述 合并（Compaction） 操作带来的文件变化。
// 在 LSM-Tree 架构中，为了提升查询性能和回收空间，系统会在后台不断地将多个小文件合并成大文件（或者将低层文件推向高层）。
// CompactIncrement 的作用就是记录一次合并任务的前后差异：
// 合并了哪些旧文件？（这些文件将不再被新快照引用）
// 产出了哪些新文件？（这些文件将加入新快照）
// 合并过程中产生了哪些 Changelog？（在某些模式下，Compaction 是生成变更日志的时机）
// 它是 Paimon 维护一致性视图的核心，确保在提交（Commit）时，系统知道哪些文件应该“下线”，哪些应该“上线”。


public class CompactIncrement {
    // 合并前的文件。
    // 这些是参与合并的原始文件。一旦合并任务提交成功，这些文件在新的快照中将被标记为“删除”。
    private final List<DataFileMeta> compactBefore;
    // 合并后的文件。这些是合并任务生成的更高层、更紧凑的新数据文件。
    private final List<DataFileMeta> compactAfter;
    // 合并产生的变更日志。当设置了 changelog-producer = lookup 或 full-compaction 时，合并过程会对比新旧数据并生成对应的变更记录文件。
    private final List<DataFileMeta> changelogFiles;
    // 新增索引文件。合并可能会导致索引（如删除向量 DV 文件或 BloomFilter 索引）被重新计算并生成新文件。
    private final List<IndexFileMeta> newIndexFiles;
    // 待删除索引文件。合并前旧数据对应的索引文件，在合并后失效。
    private final List<IndexFileMeta> deletedIndexFiles;

    public CompactIncrement(
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> changelogFiles) {
        this(compactBefore, compactAfter, changelogFiles, new ArrayList<>(), new ArrayList<>());
    }

    public CompactIncrement(
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> changelogFiles,
            List<IndexFileMeta> newIndexFiles,
            List<IndexFileMeta> deletedIndexFiles) {
        this.compactBefore = compactBefore;
        this.compactAfter = compactAfter;
        this.changelogFiles = changelogFiles;
        this.newIndexFiles = newIndexFiles;
        this.deletedIndexFiles = deletedIndexFiles;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
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
        return compactBefore.isEmpty()
                && compactAfter.isEmpty()
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

        CompactIncrement that = (CompactIncrement) o;
        return Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter)
                && Objects.equals(changelogFiles, that.changelogFiles)
                && Objects.equals(newIndexFiles, that.newIndexFiles)
                && Objects.equals(deletedIndexFiles, that.deletedIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compactBefore, compactAfter, changelogFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "CompactIncrement {compactBefore = %s, compactAfter = %s, changelogFiles = %s, newIndexFiles = %s, deletedIndexFiles = %s}",
                compactBefore.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                compactAfter.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                changelogFiles.stream().map(DataFileMeta::fileName).collect(Collectors.toList()),
                newIndexFiles.stream().map(IndexFileMeta::fileName).collect(Collectors.toList()),
                deletedIndexFiles.stream()
                        .map(IndexFileMeta::fileName)
                        .collect(Collectors.toList()));
    }

    public static CompactIncrement emptyIncrement() {
        return new CompactIncrement(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
}

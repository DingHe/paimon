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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.Closeable;
import java.util.List;

/** Rewrite sections to new level. */
// CompactRewriter 接口定义了如何物理执行数据合并和重写的标准。
// 如果说 CompactStrategy 是决定“合并谁”的大脑，那么 CompactRewriter 就是执行“怎么写”的双手。
// 核心作用是执行数据的重新组织与持久化。
// 在 LSM-Tree 架构中，合并（Compaction）不仅仅是文件的移动，它通常涉及以下复杂操作：
//多路归并排序：读取多个有序文件（Sorted Runs），将数据按主键重新排序。
//数据清理：根据业务逻辑（如 dropDelete）物理删除标记为删除的记录，或者合并相同主键的多个版本。
//文件重写：将处理后的数据按照 Paimon 的文件格式（如 ORC, Parquet）重新写出到目标层级（Level）。
//元数据更新：生成合并前后的文件对比结果（CompactResult），供后续事务提交。
public interface CompactRewriter extends Closeable {

    /**
     * Rewrite sections to new level.
     *
     * @param outputLevel new level
     * @param dropDelete whether to drop the deletion, see {@link
     *     MergeTreeCompactManager#triggerCompaction}
     * @param sections list of sections (section is a list of {@link SortedRun}s, and key intervals
     *     between sections do not overlap)
     * @return compaction result
     * @throws Exception exception
     */
    // 对一组数据段（Sections）进行深度的重写合并
    // outputLevel：合并后产生的新文件所属的层级索引。
    // dropDelete：是否在合并过程中物理丢弃删除记录（DELETE 类型的消息）。
    // 原理：在 LSM-Tree 中，如果数据被推送到最大层（Max Level），则可以安全地彻底删除“删除标记”，因为更底层不可能再有旧版本的数据。
    // sections：待合并的数据集合。Paimon 将待合并的 Runs 划分为多个 Section
    // 每个 Section 包含多个 SortedRun，但 Section 与 Section 之间的键范围（Key Interval）是不重叠的。这种设计允许 Paimon 并行地处理不同的 Section。
    CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
            throws Exception;

    /**
     * Upgrade file to new level, usually file data is not rewritten, only the metadata is updated.
     * But in some certain scenarios, we must rewrite file too, e.g. {@link
     * ChangelogMergeTreeRewriter}
     *
     * @param outputLevel new level
     * @param file file to be updated
     * @return compaction result
     * @throws Exception exception
     */
    // 将单个文件“升级”到更高的层级。
    // outputLevel：目标层级。
    // file：当前文件元数据。
    // 元数据升级（快速）：在大多数实现中，如果文件内容不需要变动，仅仅是层级变高，
    // 系统只更新文件的 level 属性而不重新读取/写入数据。这极大地减少了 IO 消耗。
    // 特殊重写（必要时）：在某些特殊实现类中（如代码注释提到的 ChangelogMergeTreeRewriter），即使是升级也可能需要重写，以产生特定的变更日志（Changelog）。
    CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception;
}

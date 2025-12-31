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

package org.apache.paimon.utils;

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;

import javax.annotation.Nullable;

/** Changes to commit. */
// Paimon 写入链路中的一个顶层容器类，负责整合一次提交任务中所有的文件变动信息。
// 作用是将“新写入的数据”与“后台合并的数据”打包在一起，形成一个完整的提交原子。
// CommitIncrement 的作用是将“新写入的数据”与“后台合并的数据”打包在一起，形成一个完整的提交原子。
// 在 Paimon 的 LSM-Tree 架构中，一个 Checkpoint 周期内通常会发生两类事情：
// 直接写入：用户发送的数据被写成了新的 L0 文件。
// 异步合并：后台线程将旧的小文件合并成了新的大文件。
// CommitIncrement 将这两类变动（DataIncrement 和 CompactIncrement）聚合，确保在提交 Snapshot（快照）时，元数据能同时感知到数据的增加和结构的优化。

public class CommitIncrement {
    // 持有本次事务中新增的数据变动。
    // 主要包含用户直接写入产生的新文件（New Files）以及可能存在的 Changelog 文件。它代表了数据的“净增长”。
    private final DataIncrement dataIncrement;
    // 持有本次事务中合并产生的变动。
    // 记录了哪些旧文件被合并了（Compact Before），以及合并后产生了哪些新文件（Compact After）。它代表了存储结构的“自我优化”。
    private final CompactIncrement compactIncrement;
    // 持有删除向量（Deletion Vector）文件的更新信息
    // 在使用删除向量模式（非主键表更新或某些特定合并引擎）时，合并操作可能会产生新的 .dv 文件。由于不是每次提交都有删除操作，该属性被标记为 @Nullable。
    @Nullable private final CompactDeletionFile compactDeletionFile;

    public CommitIncrement(
            DataIncrement dataIncrement,
            CompactIncrement compactIncrement,
            @Nullable CompactDeletionFile compactDeletionFile) {
        this.dataIncrement = dataIncrement;
        this.compactIncrement = compactIncrement;
        this.compactDeletionFile = compactDeletionFile;
    }

    public DataIncrement newFilesIncrement() {
        return dataIncrement;
    }

    public CompactIncrement compactIncrement() {
        return compactIncrement;
    }

    @Nullable
    public CompactDeletionFile compactDeletionFile() {
        return compactDeletionFile;
    }

    @Override
    public String toString() {
        return dataIncrement.toString() + "\n" + compactIncrement + "\n" + compactDeletionFile;
    }
}

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

import org.apache.paimon.KeyValue;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Changelog and final result for the same key. */
// 主要用于在 合并（Merge） 过程中同时维护两个维度的信息：最终的 状态结果 以及产生的 中间变更日志（Changelog）。
// ChangelogResult 的作用是 “同一主键在合并操作中的产物容器”。
// 在 Paimon 的主键表中，当多条具有相同主键的记录进行合并时，系统不仅需要知道最后合并出的那条数据是什么（即 result），往往还需要产生一系列的 Changelog（如 UPDATE_BEFORE 和 UPDATE_AFTER），以便下游的流式作业（如 Flink）能够感知到增量变化。
// 该类将这两部分数据封装在一起，作为一次合并操作的完整输出。

public class ChangelogResult {
    // 存储该主键在本次合并过程中产生的变更记录。
    // 这些记录会被发送到 Changelog 文件中。例如，如果一次合并涉及更新，这里可能包含一条 UPDATE_BEFORE 和一条 UPDATE_AFTER 记录。
    private final List<KeyValue> changelogs = new ArrayList<>();
    // 存储合并后的最终状态结果。
    @Nullable private KeyValue result;
    // 清空当前对象的内部状态。
    public void reset() {
        changelogs.clear();
        result = null;
    }
    // 向变更日志列表中添加一条记录。
    public ChangelogResult addChangelog(KeyValue record) {
        changelogs.add(record);
        return this;
    }
    // 只有当记录不是“回撤（Retraction）”类型时，才设置最终结果。
    public ChangelogResult setResultIfNotRetract(@Nullable KeyValue result) {
        if (result != null
                && result.valueKind() != RowKind.DELETE
                && result.valueKind() != RowKind.UPDATE_BEFORE) {
            setResult(result);
        }
        return this;
    }

    public ChangelogResult setResult(@Nullable KeyValue result) {
        this.result = result;
        return this;
    }

    public List<KeyValue> changelogs() {
        return changelogs;
    }

    /** Latest result (result of merge function) for this key. */
    @Nullable
    public KeyValue result() {
        return result;
    }
}

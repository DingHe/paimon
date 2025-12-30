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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG_NO_REWRITE;

/** A {@link MergeTreeCompactRewriter} which produces changelog files for each full compaction. */
// FullChangelogMergeTreeCompactRewriter 是一个专门用于处理 全量合并（Full Compaction） 并生成 完整变更日志（Full Changelog） 的重写器。
// 与之前讨论的基于 Lookup 的重写器不同，它不依赖外部索引，而是在所有层级数据参与的大合并中产出 Changelog。
// 在执行 Full Compaction（将所有层级的数据合并到最大层 maxLevel）时，计算并产出 Changelog 文件。
// 在 Paimon 中，如果设置了 changelog-producer = full-compaction，每当触发全量合并时，系统会对比旧的最大层数据和新合并进来的数据。
//如果某行数据是新出现的，生成 INSERT 日志。
//如果某行数据被更新了，生成 UPDATE_BEFORE 和 UPDATE_AFTER。
//如果某行数据被删除了，生成 DELETE。
//由于它是在全量合并时进行的，因此它能保证产生最精确、无冗余的变更流，常用于对数据正确性要求极高且能容忍全量合并开销的场景。

public class FullChangelogMergeTreeCompactRewriter extends ChangelogMergeTreeRewriter {

    @Nullable private final RecordEqualiser valueEqualiser;

    public FullChangelogMergeTreeCompactRewriter(
            int maxLevel,
            CoreOptions.MergeEngine mergeEngine,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            @Nullable RecordEqualiser valueEqualiser) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                true,
                false);
        this.valueEqualiser = valueEqualiser;
    }

    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        boolean changelog = outputLevel == maxLevel;
        if (changelog) {
            Preconditions.checkArgument(
                    dropDelete,
                    "Delete records should be dropped from result of full compaction. This is unexpected.");
        }
        return changelog;
    }

    @Override
    protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
        return outputLevel == maxLevel ? CHANGELOG_NO_REWRITE : NO_CHANGELOG_NO_REWRITE;
    }

    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return new FullChangelogMergeFunctionWrapper(mfFactory.create(), maxLevel, valueEqualiser);
    }

    @Override
    public void close() throws IOException {}
}

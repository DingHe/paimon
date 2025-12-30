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

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files while performing compaction.
 */
// ChangelogMergeTreeRewriter 是一个非常关键的抽象类。它继承自 MergeTreeCompactRewriter，
// 其最核心的特性是：在执行 LSM-Tree 合并重写的同时，能够捕获数据的变更（New Value vs Old Value），并生成 Changelog（变更日志）文件。
// 这使得 Paimon 能够支持高效的流式增量消费（例如 Flink SQL 消费 Paimon 表的变更数据）
// 该类的核心任务是**“一石二鸟”**：
// 物理合并：像普通重写器一样，将多个层级的文件合并成一个紧凑的文件。
// 生成增量日志：通过特定的 MergeFunction 包装器，计算出合并前后的差异（例如：这一行是新增的 INSERT，还是更新后的 UPDATE_AFTER），并将这些差异写出到独立的 .changelog 文件中。

public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {
    // LSM-Tree 的最大层级索引。用于判断是否合并到了最底层
    protected final int maxLevel;
    // 合并引擎类型（如 DEDUPLICATE、PARTIAL_UPDATE 或 AGGREGATE）。不同的引擎计算 Changelog 的逻辑不同。
    protected final MergeEngine mergeEngine;
    // 标识当前配置是否需要产生 Changelog 文件
    private final boolean produceChangelog;
    // 标识是否强制丢弃 DELETE 类型的消息（通常用于特定层级的清理）。
    private final boolean forceDropDelete;

    public ChangelogMergeTreeRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            boolean produceChangelog,
            boolean forceDropDelete) {
        super(
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter);
        this.maxLevel = maxLevel;
        this.mergeEngine = mergeEngine;
        this.produceChangelog = produceChangelog;
        this.forceDropDelete = forceDropDelete;
    }
    // 判断在当前的 outputLevel 和 sections 条件下，是否必须通过重写来生成 Changelog。
    protected abstract boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);
    // 升级策略。
    // 决定文件从低层升到高层时，是直接改元数据（Fast），还是必须重写以补全 Changelog
    protected abstract UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file);
    // 核心工厂方法。
    // 创建一个能产生 ChangelogResult 的合并包装器。这是计算“新旧值差异”的逻辑所在。
    protected abstract MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel);

    // 如果输出层不是 Level 0，且输入的文件中包含 Level 0 的文件，则返回 true。
    // 这意味着数据正在从无序的 L0 进入有序层，通常需要进行 Lookup 以确定是插入还是更新。
    protected boolean rewriteLookupChangelog(int outputLevel, List<List<SortedRun>> sections) {
        if (outputLevel == 0) {
            return false;
        }

        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    if (file.level() == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        // 如果需要生成 Changelog，调用 rewriteOrProduceChangelog
        if (rewriteChangelog(outputLevel, dropDelete, sections)) {
            return rewriteOrProduceChangelog(outputLevel, sections, dropDelete, true);
        } else {
        // 如果不需要（比如只是纯粹的物理合并），调用父类的 rewriteCompaction
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }
    }

    /**
     * Rewrite or produce changelog at the same time.
     *
     * @param dropDelete whether to drop delete when rewrite compact file
     * @param rewriteCompactFile whether to rewrite compact file
     */
    // Paimon 实现流批一体的核心逻辑。它在一次扫描中同时完成“数据合并重写”和“变更日志计算”两项任务。
    //
    private CompactResult rewriteOrProduceChangelog(
            int outputLevel,
            List<List<SortedRun>> sections,
            boolean dropDelete,
            boolean rewriteCompactFile)
            throws Exception {
        // 声明迭代器、合并数据文件写入器、Changelog 文件写入器以及异常收集器。
        // 使用 RollingFileWriter 是为了支持当文件达到设定大小时自动滚动产生新文件
        CloseableIterator<ChangelogResult> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;
        Exception collectedExceptions = null;

        try {
            // createMergeWrapper 会根据当前引擎（如 Deduplicate）创建一个能对比新旧数据并生成 ChangelogResult 的包装器。
            // 通过归并排序，数据以“主键变更结果”的形式流出。
            iterator =
                    readerForMergeTree(sections, createMergeWrapper(outputLevel))
                            .toCloseableIterator();
            // 如果需要物理重写合并后的文件（通常都需要），则创建对应层级的数据文件写入器
            if (rewriteCompactFile) {
                compactFileWriter =
                        writerFactory.createRollingMergeTreeFileWriter(
                                outputLevel, FileSource.COMPACT);
            }
            // 如果配置开启了 Changelog 产生，则创建专门的 .changelog 格式文件写入器
            if (produceChangelog) {
                changelogFileWriter = writerFactory.createRollingChangelogFileWriter(outputLevel);
            }

            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                KeyValue keyValue = result.result();
                // 写入合并后的正式数据文件
                // 如果 dropDelete 为 true（通常在合并到最大层时），则会过滤掉 DELETE 类型的消息，实现物理删除。
                if (compactFileWriter != null
                        && keyValue != null
                        && (!dropDelete || keyValue.isAdd())) {
                    compactFileWriter.write(keyValue);
                }
                // 将计算出的变更（如 UPDATE_BEFORE 和 UPDATE_AFTER）写入 Changelog 文件，供下游流式作业消费。
                if (produceChangelog) {
                    for (KeyValue kv : result.changelogs()) {
                        changelogFileWriter.write(kv);
                    }
                }
            }
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(iterator, compactFileWriter, changelogFileWriter);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }
        // 事务原子性保证。
        // 如果执行过程中出错，立即调用 abort() 物理删除已生成的临时文件，防止产生脏数据。
        if (null != collectedExceptions) {
            if (compactFileWriter != null) {
                compactFileWriter.abort();
            }
            if (changelogFileWriter != null) {
                changelogFileWriter.abort();
            }
            throw collectedExceptions;
        }

        // 获取合并前的文件列表（即被替换掉的文件）
        List<DataFileMeta> before = extractFilesFromSections(sections);
        // 确定合并后的新文件列表
        // 如果没重写文件（例如仅产生 Changelog），则将原文件元数据通过 upgrade 提升层级作为结果。
        List<DataFileMeta> after =
                compactFileWriter != null
                        ? compactFileWriter.result()
                        : before.stream()
                                .map(x -> x.upgrade(outputLevel))
                                .collect(Collectors.toList());
        // 触发合并前后的钩子函数（Hook），用于子类扩展自定义逻辑
        if (rewriteCompactFile) {
            notifyRewriteCompactBefore(before);
        }

        after = notifyRewriteCompactAfter(after);

        List<DataFileMeta> changelogFiles =
                changelogFileWriter != null
                        ? changelogFileWriter.result()
                        : Collections.emptyList();
        return new CompactResult(before, after, changelogFiles);
    }
    // 目的是为了在文件“晋升”时强制补齐可能缺失的 Changelog。
    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        // 根据当前文件所属的层级、目标层级（outputLevel）以及合并引擎的状态，决定采用哪种升级方案。
        // 背景：在 Paimon 中，某些层级的文件可能已经产生过 Changelog，而某些层级（如刚刚 Flush 下来的 L0）可能还没有。
        // 该策略会告诉系统：这个文件升层时是否必须补产一条变更日志。
        UpgradeStrategy strategy = upgradeStrategy(outputLevel, file);
        // 判断获取到的策略中 changelog 属性是否为 true
        if (strategy.changelog) {
            return rewriteOrProduceChangelog(
                    outputLevel,
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))),
                    forceDropDelete,
                    strategy.rewrite);
        } else {
            return super.upgrade(outputLevel, file);
        }
    }

    /** Strategy for upgrade. */
    protected enum UpgradeStrategy {
        NO_CHANGELOG_NO_REWRITE(false, false),
        CHANGELOG_NO_REWRITE(true, false),
        CHANGELOG_WITH_REWRITE(true, true);

        private final boolean changelog;
        private final boolean rewrite;

        UpgradeStrategy(boolean changelog, boolean rewrite) {
            this.changelog = changelog;
            this.rewrite = rewrite;
        }
    }
}

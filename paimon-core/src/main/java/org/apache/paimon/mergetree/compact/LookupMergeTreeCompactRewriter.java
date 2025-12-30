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
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG_NO_REWRITE;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files by lookup for the compaction
 * involving level 0 files.
 */
// 专门用于在 Lookup 合并模式下执行数据重写和 Changelog 生成。
// 在 Paimon 中，如果配置了 changelog-producer = lookup，系统在合并（Compaction）时需要知道某条数据在“更底层”是否存在，
// 以便准确判断它是 INSERT（新插入）还是 UPDATE（更新）。
// 点查辅助合并：在合并 Level 0 文件或执行跨层合并时，通过 LookupLevels（通常是内存索引或本地 SST 索引）去更高层级检索主键是否存在。
// 生成高精度 Changelog：基于 Lookup 的结果，它可以产出包含 UPDATE_BEFORE 的完整变更流。
// 维护删除向量 (Deletion Vector)：如果开启了 DV 模式，它负责在重写过程中更新或清理相关的删除标记。
// 支持远程索引：配合 RemoteLookupFileManager，支持将索引信息维护在远程存储。

public class LookupMergeTreeCompactRewriter<T> extends ChangelogMergeTreeRewriter {
    // 封装了对 LSM-Tree 历史层级的点查逻辑，用于判断 Key 是否存在及其旧值。
    private final LookupLevels<T> lookupLevels;
    // 用于创建 MergeFunctionWrapper，
    // 它是处理合并逻辑和生成 ChangelogResult 的地方。
    private final MergeFunctionWrapperFactory<T> wrapperFactory;
    // 标识表中是否定义了序列号字段。如果没有序列号，DEDUPLICATE 引擎的升级策略可以优化。
    private final boolean noSequenceField;
    // 删除向量维护者。
    // 处理物理删除标记的增删。
    @Nullable private final BucketedDvMaintainer dvMaintainer;
    // 函数式接口。
    // 根据层级获取文件格式（例如某些层级用 ORC，某些用 Parquet）。
    private final IntFunction<String> level2FileFormat;
    // 远程 Lookup 文件管理器。
    // 用于在分布式环境下管理和生成 Lookup 所需的索引文件。
    @Nullable private final RemoteLookupFileManager<T> remoteLookupFileManager;

    public LookupMergeTreeCompactRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            LookupLevels<T> lookupLevels,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            MergeFunctionWrapperFactory<T> wrapperFactory,
            boolean produceChangelog,
            @Nullable BucketedDvMaintainer dvMaintainer,
            CoreOptions options,
            @Nullable RemoteLookupFileManager<T> remoteLookupFileManager) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                produceChangelog,
                dvMaintainer != null);
        this.dvMaintainer = dvMaintainer;
        this.lookupLevels = lookupLevels;
        this.wrapperFactory = wrapperFactory;
        this.noSequenceField = options.sequenceField().isEmpty();
        String fileFormat = options.fileFormatString();
        Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
        this.level2FileFormat = level -> fileFormatPerLevel.getOrDefault(level, fileFormat);
        this.remoteLookupFileManager = remoteLookupFileManager;
    }

    @Override
    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {
        if (dvMaintainer != null) {
            files.forEach(file -> dvMaintainer.removeDeletionVectorOf(file.fileName()));
        }
    }

    @Override
    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        if (remoteLookupFileManager == null) {
            return files;
        }

        List<DataFileMeta> result = new ArrayList<>();
        for (DataFileMeta file : files) {
            try {
                result.add(remoteLookupFileManager.genRemoteLookupFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }
    // 决定在压缩（Compaction）过程中是否需要物理重写并生成 Changelog 的入口。
    // 判定本次 Compaction 任务是否属于“必须重写数据以生成变更日志”的场景。
    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        return rewriteLookupChangelog(outputLevel, sections);
    }
    // 当一个文件从低层级“升级”到高层级时，是否需要生成 Changelog（变更日志），以及是否需要物理上重写该文件。
    @Override
    protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
        // 非 0 层文件的快速返回
        // 检查文件的原始层级。如果文件不是来自 Level 0，则直接返回“不生成 Changelog 且不重写”。
        // 在 LSM-Tree 中，Level 0 以外的层级通常已经是有序且去重的。如果只是单纯的层级提升（Upgrade），且不是从 L0 开始，通常不需要重新处理数据。
        if (file.level() != 0) {
            return NO_CHANGELOG_NO_REWRITE;
        }

        // forcing rewriting when upgrading from level 0 to level x with different file formats
        // 文件格式不一致触发重写
        // 比较原始层级和目标层级的文件格式（如 Parquet 或 ORC）。
        // 如果 Level 0 使用的存储格式与目标层级（outputLevel）不同，则必须物理重写文件。此时会返回 CHANGELOG_WITH_REWRITE，表示需要生成变更日志并重写数据。
        if (!level2FileFormat.apply(file.level()).equals(level2FileFormat.apply(outputLevel))) {
            return CHANGELOG_WITH_REWRITE;
        }

        // In deletionVector mode, since drop delete is required, when delete row count > 0 rewrite
        // is required.
        // 判断是否处于 DV 模式，且文件中是否存在已删除的行。
        // 如果启用了删除向量（dvMaintainer != null），为了彻底清理（Drop）那些被标记为删除的行，如果文件中存在删除记录（或无法确定删除数量时），必须通过重写文件来完成物理删除。
        if (dvMaintainer != null && file.deleteRowCount().map(cnt -> cnt > 0).orElse(true)) {
            return CHANGELOG_WITH_REWRITE;
        }
        // 如果文件直接升级到了 LSM-Tree 的最底层（Max Level）。
        // 到达最底层意味着数据已经处于最终态。Paimon 认为此时不需要重写数据内容，只需要生成 Changelog 来通知下游系统数据已落盘即可。
        if (outputLevel == maxLevel) {
            return CHANGELOG_NO_REWRITE;
        }

        // DEDUPLICATE retains the latest records as the final result, so merging has no impact on
        // it at all.
        // 判断合并引擎是否为 DEDUPLICATE（去重）且没有配置序列字段（Sequence Field）。
        // 去重引擎只保留主键对应的最新一条数据。如果没有序列字段来定义优先级，合并操作对最终结果没有影响。
        // 因此，只需生成 Changelog，无需浪费 IO 去物理重写文件。
        if (mergeEngine == MergeEngine.DEDUPLICATE && noSequenceField) {
            return CHANGELOG_NO_REWRITE;
        }

        // other merge engines must rewrite file, because some records that are already at higher
        // level may be merged
        // See LookupMergeFunction, it just returns newly records.
        // 对于其他的合并引擎（如 PartialUpdate 或 Aggregation），必须重写文件。
        return CHANGELOG_WITH_REWRITE;
    }

    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return wrapperFactory.create(mfFactory, outputLevel, lookupLevels, dvMaintainer);
    }

    @Override
    public void close() throws IOException {
        lookupLevels.close();
    }

    /** Factory to create {@link MergeFunctionWrapper}. */
    // 负责将复杂的 Lookup 组件（如 LookupLevels、mfFactory）组装成一个能够处理数据合并并产生 ChangelogResult（变更日志结果）的运行时对象。
    public interface MergeFunctionWrapperFactory<T> {

        MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer);
    }

    /** A normal {@link MergeFunctionWrapperFactory} to create lookup wrapper. */
    // 通用 Lookup 合并工厂。用于处理大多数合并引擎（如 DEDUPLICATE、PARTIAL_UPDATE、AGGREGATE）。
    // 创建一个能够从高层级（outputLevel + 1）查找旧数据，并与当前数据进行对比、合并，最终生成更新前（Before）和更新后（After）快照的包装器。
    public static class LookupMergeFunctionWrapperFactory<T>
            implements MergeFunctionWrapperFactory<T> {

        @Nullable private final RecordEqualiser valueEqualiser;
        private final LookupStrategy lookupStrategy;
        @Nullable private final UserDefinedSeqComparator userDefinedSeqComparator;

        public LookupMergeFunctionWrapperFactory(
                @Nullable RecordEqualiser valueEqualiser,
                LookupStrategy lookupStrategy,
                @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
            this.valueEqualiser = valueEqualiser;
            this.lookupStrategy = lookupStrategy;
            this.userDefinedSeqComparator = userDefinedSeqComparator;
        }

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
            // 返回 LookupChangelogMergeFunctionWrapper。这个对象在运行时会执行：读取 -> Lookup 查找旧值 -> 执行 MergeFunction -> 产生 Changelog。
            return new LookupChangelogMergeFunctionWrapper<>(
                    mfFactory,
                    // 如果你想知道这个 Key 的旧值，就去比当前输出层级更深（+1）的层级去找。
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    valueEqualiser,
                    lookupStrategy,
                    deletionVectorsMaintainer,
                    userDefinedSeqComparator);
        }
    }

    /** A {@link MergeFunctionWrapperFactory} for first row. */
    // 首行（First Row）合并工厂。
    // 专门用于 first-row 引擎。在这种模式下，Paimon 只保留主键第一次出现的那一行。因此，工厂创建的包装器逻辑非常简单：只需判断主键是否在更高层级存在即可（存在即丢弃当前行）。
    public static class FirstRowMergeFunctionWrapperFactory
            implements MergeFunctionWrapperFactory<Boolean> {

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<Boolean> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
            return new FirstRowMergeFunctionWrapper(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1) != null;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}

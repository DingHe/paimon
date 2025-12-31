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

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Compact manager for {@link KeyValueFileStore}. */
// MergeTreeCompactManager 是负责管理 Merge Tree 压缩（Compaction） 逻辑的核心组件。
// 它通过协调策略选择、任务提交和结果更新，确保 LSM-Tree 结构的健康和查询性能。
// 核心作用是 “LSM-Tree 的管家”：
// 状态维护：持有并管理 Levels（层级管理器），实时更新当前有哪些数据文件。
// 策略执行：根据配置的 CompactStrategy（如 Universal Compaction）决定什么时候、哪些文件需要被压缩。
// 异步调度：将选中的文件封装成 CompactTask，提交到线程池异步执行，避免阻塞写入主线程。
// 结果应用：当压缩完成后，将生成的新文件更新到 Levels 中，并清理掉已被合并的旧文件。
// 反压控制：通过监控 L0 层文件的数量，告知上层是否需要减缓写入速度（Stop Trigger）
public class MergeTreeCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeCompactManager.class);
    // 执行压缩任务的线程池
    private final ExecutorService executor;
    // 记录 LSM-Tree 每一层的文件信息，是压缩的数据基础。
    private final Levels levels;
    // 压缩策略，定义了“挑文件”的算法。
    private final CompactStrategy strategy;
    private final Comparator<InternalRow> keyComparator;
    private final long compactionFileSize;
    // 触发停止写入的阈值。
    // 当 Sorted Run 数量过多时，系统会强制等待压缩。
    private final int numSortedRunStopTrigger;
    // 实际执行文件合并、去重、序列化写入的工具。
    private final CompactRewriter rewriter;
    // 监控指标报告器，记录压缩次数、文件数量等。
    @Nullable private final CompactionMetrics.Reporter metricsReporter;
    // 删除向量（Deletion Vector）维护者，用于支持存量数据的物理删除。
    @Nullable private final BucketedDvMaintainer dvMaintainer;
    // 是否延迟生成删除文件，以优化写性能。
    private final boolean lazyGenDeletionFile;
    // 标记是否需要 Lookup 逻辑。
    private final boolean needLookup;
    // 是否强制重写所有文件。
    private final boolean forceRewriteAllFiles;
    // 记录级别的过期策略，在压缩时顺便清理掉过期数据。
    @Nullable private final RecordLevelExpire recordLevelExpire;

    public MergeTreeCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long compactionFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            @Nullable BucketedDvMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            boolean needLookup,
            @Nullable RecordLevelExpire recordLevelExpire,
            boolean forceRewriteAllFiles) {
        this.executor = executor;
        this.levels = levels;
        this.strategy = strategy;
        this.compactionFileSize = compactionFileSize;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.keyComparator = keyComparator;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;
        this.recordLevelExpire = recordLevelExpire;
        this.needLookup = needLookup;
        this.forceRewriteAllFiles = forceRewriteAllFiles;

        MetricUtils.safeCall(this::reportMetrics, LOG);
    }
    // 判断是否需要等待当前的压缩任务完成。
    // 当 Sorted Run 数量超过配置的触发器阈值时返回 true，用于防止 L0 文件堆积导致的查询缓慢。
    @Override
    public boolean shouldWaitForLatestCompaction() {
        return levels.numberOfSortedRuns() > numSortedRunStopTrigger;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        // cast to long to avoid Numeric overflow
        return levels.numberOfSortedRuns() > (long) numSortedRunStopTrigger + 1;
    }
    // 向管理器注册新生成的（通常是 Commit 提交的）文件
    @Override
    public void addNewFile(DataFileMeta file) {
        // if overwrite an empty partition, the snapshot will be changed to APPEND, then its files
        // might be upgraded to high level, thus we should use #update
        levels.update(Collections.emptyList(), Collections.singletonList(file));
        MetricUtils.safeCall(this::reportMetrics, LOG);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }
    // 压缩机制的“发动机”。它负责根据当前 LSM-Tree 的状态和用户需求，决定是否需要启动压缩以及选择哪些文件进行压缩。
    @Override
    public void triggerCompaction(boolean fullCompaction) {
        // 定义压缩单元变量 optionalUnit（代表一组选中的文件和目标层级）。
        // 同时从 levels 管理器中获取当前所有层级的有序文件集合（LevelSortedRun）
        Optional<CompactUnit> optionalUnit;
        List<LevelSortedRun> runs = levels.levelSortedRuns();
        // 强制全量压缩 (Full Compaction)
        if (fullCompaction) {
            // Paimon 不允许在已有压缩任务运行时强制开启新的全量压缩，否则会抛出异常。
            Preconditions.checkState(
                    taskFuture == null,
                    "A compaction task is still running while the user "
                            + "forces a new compaction. This is unexpected.");
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Trigger forced full compaction. Picking from the following runs\n{}",
                        runs);
            }
            optionalUnit =
                    CompactStrategy.pickFullCompaction(
                            levels.numberOfLevels(),
                            runs,
                            recordLevelExpire,
                            dvMaintainer,
                            forceRewriteAllFiles);
        } else {
            // 普通压缩 (Normal Compaction)

            // 对于普通压缩，如果当前已经有一个压缩任务在后台运行，则直接返回，不再触发新的任务（保证单任务并发控制）。
            if (taskFuture != null) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trigger normal compaction. Picking from the following runs\n{}", runs);
            }
            optionalUnit =
                    strategy.pick(levels.numberOfLevels(), runs)
                            .filter(unit -> !unit.files().isEmpty()) // 文件不能为空。
                        // 必须满足以下之一：参与合并的文件超过 1 个，或者虽然只有 1 个文件但它的当前层级与目标层级不一致（即“层级升级”）。
                            .filter(
                                    unit ->
                                            unit.files().size() > 1
                                                    || unit.files().get(0).level()
                                                            != unit.outputLevel());
        }

        optionalUnit.ifPresent(
                unit -> {
                    /*
                     * As long as there is no older data, We can drop the deletion.
                     * If the output level is 0, there may be older data not involved in compaction.
                     * If the output level is bigger than 0, as long as there is no older data in
                     * the current levels, the output is the oldest, so we can drop the deletion.
                     * See CompactStrategy.pick.
                     */
                    // 确定是否可以丢弃删除记录 (dropDelete)
                    // 目标层级不能是 Level 0（因为 L0 是乱序的，可能有更老的数据在下面）。
                    // 标层级已经是当前最底层（说明下面没有更老的数据了），或者使用了 dvMaintainer（删除向量模式下可以物理丢弃被标记的数据）。
                    // 如果确认该记录是“全宇宙”最老的，那么 DELETE 标记就可以彻底物理删除，不再向下传递。
                    boolean dropDelete =
                            unit.outputLevel() != 0
                                    && (unit.outputLevel() >= levels.nonEmptyHighestLevel()
                                            || dvMaintainer != null);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Submit compaction with files (name, level, size): "
                                        + levels.levelSortedRuns().stream()
                                                .flatMap(lsr -> lsr.run().files().stream())
                                                .map(
                                                        file ->
                                                                String.format(
                                                                        "(%s, %d, %d)",
                                                                        file.fileName(),
                                                                        file.level(),
                                                                        file.fileSize()))
                                                .collect(Collectors.joining(", ")));
                    }
                    // 正式将任务提交到线程池异步执行。
                    submitCompaction(unit, dropDelete);
                });
    }

    @VisibleForTesting
    public Levels levels() {
        return levels;
    }

    private void submitCompaction(CompactUnit unit, boolean dropDelete) {
        Supplier<CompactDeletionFile> compactDfSupplier = () -> null;
        if (dvMaintainer != null) {
            compactDfSupplier =
                    lazyGenDeletionFile
                            ? () -> CompactDeletionFile.lazyGeneration(dvMaintainer)
                            : () -> CompactDeletionFile.generateFiles(dvMaintainer);
        }

        CompactTask task;
        if (unit.fileRewrite()) {
            task = new FileRewriteCompactTask(rewriter, unit, dropDelete, metricsReporter);
        } else {
            task =
                    new MergeTreeCompactTask(
                            keyComparator,
                            compactionFileSize,
                            rewriter,
                            unit,
                            dropDelete,
                            levels.maxLevel(),
                            metricsReporter,
                            compactDfSupplier,
                            recordLevelExpire,
                            forceRewriteAllFiles);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Pick these files (name, level, size) for {} compaction: {}",
                    task.getClass().getSimpleName(),
                    unit.files().stream()
                            .map(
                                    file ->
                                            String.format(
                                                    "(%s, %d, %d)",
                                                    file.fileName(), file.level(), file.fileSize()))
                            .collect(Collectors.joining(", ")));
        }
        taskFuture = executor.submit(task);
        if (metricsReporter != null) {
            metricsReporter.increaseCompactionsQueuedCount();
            metricsReporter.increaseCompactionsTotalCount();
        }
    }

    /** Finish current task, and update result files to {@link Levels}. */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Update levels in compact manager with these changes:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    levels.update(r.before(), r.after());
                    MetricUtils.safeCall(this::reportMetrics, LOG);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    @Override
    public boolean compactNotCompleted() {
        // If it is a lookup compaction, we should ensure that all level 0 files are consumed, so
        // here we need to make the outside think that we still need to do unfinished compact
        // working
        return super.compactNotCompleted() || (needLookup && !levels().level0().isEmpty());
    }

    private void reportMetrics() {
        if (metricsReporter != null) {
            metricsReporter.reportLevel0FileCount(levels.level0().size());
            metricsReporter.reportTotalFileSize(levels.totalFileSize());
        }
    }

    @Override
    public void close() throws IOException {
        rewriter.close();
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG);
        }
    }

    @VisibleForTesting
    public CompactStrategy getStrategy() {
        return strategy;
    }
}

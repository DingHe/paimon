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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StreamScanMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.snapshot.AllDeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.ChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingContext;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.NextSnapshotFetcher;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;
import static org.apache.paimon.CoreOptions.ChangelogProducer.LOOKUP;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;

/** {@link StreamTableScan} implementation for streaming planning. */
// DataTableStreamScan 是实现流式增量扫描的核心类。它负责在流式作业中不断地探测新快照（Snapshot），并根据表的配置（如是否产生 Changelog）生成增量读取计划。
// DataTableStreamScan 充当了 Paimon 流式读取的“执行状态机”。它的主要职责包括：
// 启动定位：决定流任务启动时是从头读、读最新，还是从指定位置读。
// 持续追踪：维护 nextSnapshotId 指针，不断探测并处理新提交的快照。
// 模式适配：根据 ChangelogProducer 的配置，决定是读取物理 .changelog 文件还是通过对比 Delta 增量文件来生成变更。
// 状态维护：支持 Checkpoint 时的进度保存（checkpoint()）与故障恢复（restore()）。
public class DataTableStreamScan extends AbstractDataTableScan implements StreamDataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableStreamScan.class);
    // 核心配置项。
    private final CoreOptions options;
    // 扫描模式（如 FILE_MONITOR）
    private final StreamScanMode scanMode;
    private final SnapshotManager snapshotManager;
    // 是否支持在流读中处理 OVERWRITE 类型的快照
    private final boolean supportStreamingReadOverwrite;
    // 封装了寻找下一个可用快照的逻辑。
    private final NextSnapshotFetcher nextSnapshotProvider;
    // 表是否有主键。
    private final boolean hasPk;
    // 标记扫描器是否已完成初始化（创建 Starting/FollowUp 扫描器）
    private boolean initialized = false;
    // 负责启动时的第一次扫描策略。
    private StartingScanner startingScanner;
    // 负责启动后的后续增量快照扫描策略。
    private FollowUpScanner followUpScanner;
    // 检查是否达到了用户定义的扫描终点（如指定的位点或时间）
    private BoundedChecker boundedChecker;
    // 标记全量读取阶段或有界读取是否已结束。
    private boolean isFullPhaseEnd = false;
    // 当前读到的数据水位线。
    @Nullable private Long currentWatermark;
    // 最核心属性，
    // 记录下一次扫描应该从哪个快照 ID 开始。
    @Nullable private Long nextSnapshotId;

    @Nullable private Long scanDelayMillis;

    public DataTableStreamScan(
            TableSchema schema,
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean supportStreamingReadOverwrite,
            TableQueryAuth queryAuth,
            boolean hasPk) {
        super(schema, options, snapshotReader, queryAuth);

        this.options = options;
        this.scanMode = options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        this.snapshotManager = snapshotManager;
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
        this.nextSnapshotProvider =
                new NextSnapshotFetcher(
                        snapshotManager, changelogManager, options.changelogLifecycleDecoupled());
        this.hasPk = hasPk;

        if (options.bucket() == BucketMode.POSTPONE_BUCKET
                && options.changelogProducer() != CoreOptions.ChangelogProducer.NONE) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    @Override
    public DataTableStreamScan withFilter(Predicate predicate) {
        super.withFilter(predicate);
        snapshotReader.withFilter(predicate);
        return this;
    }

    @Override
    public StartingContext startingContext() {
        if (!initialized) {
            initScanner();
        }
        return startingScanner.startingContext();
    }

    @Override
    public Plan plan() {
        authQuery();

        if (!initialized) {
            initScanner();
        }

        if (nextSnapshotId == null) {
            return tryFirstPlan();
        } else {
            return nextPlan();
        }
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        throw new UnsupportedOperationException(
                "List Partition Entries is not supported in Stream Scan.");
    }

    private void initScanner() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }
        if (boundedChecker == null) {
            boundedChecker = createBoundedChecker();
        }
        if (scanDelayMillis == null) {
            scanDelayMillis = getScanDelayMillis();
        }
        initialized = true;
    }
    // 处理流式作业启动时的“第一跳”。
    // 它决定了作业是从历史快照开始读（全量），还是直接跳到某个位置开始监听增量。
    private Plan tryFirstPlan() {
        StartingScanner.Result result;
        // 如果扫描模式是 FILE_MONITOR（文件监控），则按常规流程执行启动扫描。
        // 通常会扫描出当前快照的所有文件。
        if (scanMode == FILE_MONITOR) {
            result = startingScanner.scan(snapshotReader);
        // 处理 LOOKUP 增量生产者模式。
        } else if (options.changelogProducer().equals(LOOKUP)) {
            // level0 data will be compacted to produce changelog in the future
            // 在启动扫描时，过滤掉 Level 0 的文件（只读 level > 0 的文件）
            // 在 Paimon 的 LSM 结构中，Level 0 的数据尚未经过整理，未来会通过 Compact 生成相应的 Changelog。
            // 为了防止流式读取时数据重复或丢失（保证增量语义），启动阶段只读取已经稳定在底层 Level 的数据。随后恢复过滤器。
            result = startingScanner.scan(snapshotReader.withLevelFilter(level -> level > 0));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        // 处理 FULL_COMPACTION 增量生产者模式。
        } else if (options.changelogProducer().equals(FULL_COMPACTION)) {
            // 启动时只读取最高层（最后一层）的文件
            // 原因：在这种模式下，只有到达最后一层的数据才被视为“已确认的增量基准”。过滤掉非最后一层的文件是为了配合后续只读 Full Compaction 产生的 Changelog 的逻辑。
            result =
                    startingScanner.scan(
                            snapshotReader.withLevelFilter(
                                    level -> level == options.numLevels() - 1));
            snapshotReader.withLevelFilter(Filter.alwaysTrue());
        } else {
        // 默认情况（如 NONE 或 INPUT 模式），直接执行扫描器逻辑。
            result = startingScanner.scan(snapshotReader);
        }
        // 有初始数据
        if (result instanceof ScannedResult) {
            ScannedResult scannedResult = (ScannedResult) result;
            // 设置当前水位线
            currentWatermark = scannedResult.currentWatermark();
            // 获取当前扫描的快照ID
            long currentSnapshotId = scannedResult.currentSnapshotId();
            // 关键：指向下一个快照
            nextSnapshotId = currentSnapshotId + 1;
            // 检查是否达到了有界读取的终点
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
            LOG.debug(
                    "Starting snapshot is {}, next snapshot will be {}.",
                    scannedResult.plan().snapshotId(),
                    nextSnapshotId);
            // 返回包含文件切片的物理执行计划
            return scannedResult.plan();
        // 直接进入增量监听
        } else if (result instanceof StartingScanner.NextSnapshot) {
            // 直接获取起跳ID
            // 对应 LATEST 模式。它不会返回任何文件，而是直接把 nextSnapshotId 设为最新的快照 ID + 1。这意味着忽略所有历史数据，从此刻开始监听新数据。
            nextSnapshotId = ((StartingScanner.NextSnapshot) result).nextSnapshotId();
            // 检查前一个快照是否已经是终点
            isFullPhaseEnd =
                    snapshotManager.snapshotExists(nextSnapshotId - 1)
                            && boundedChecker.shouldEndInput(
                                    snapshotManager.snapshot(nextSnapshotId - 1));
            LOG.debug("There is no starting snapshot. Next snapshot will be {}.", nextSnapshotId);
        } else if (result instanceof StartingScanner.NoSnapshot) {
            LOG.debug("There is no starting snapshot and currently there is no next snapshot.");
        }
        return SnapshotNotExistPlan.INSTANCE;
    }
    // nextPlan 方法是 DataTableStreamScan 的“心脏”。
    // 在流式作业启动并执行完 tryFirstPlan 之后，该方法会被循环调用，负责持续监听、过滤并拉取后续产生的新快照增量数据。
    private Plan nextPlan() {
        while (true) {
            // 检查扫描是否已经结束。
            if (isFullPhaseEnd) {
                throw new EndOfScanException();
            }
            // 探测与拉取下一个快照
            Snapshot snapshot = nextSnapshotProvider.getNextSnapshot(nextSnapshotId);
            // 如果快照尚未产生，直接返回“不存在”信号。
            // 这会导致流作业暂时挂起（休眠一段时间后再重新调用 plan()），等待上游写入端提交新的快照。
            if (snapshot == null) {
                return SnapshotNotExistPlan.INSTANCE;
            }
            // 有界性检查与延迟处理
            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }
            // 实现“延迟读取”功能。如果配置了读取延迟，即使快照已经产生，只要它太“新”（未达到冷却时间），就不处理，返回空计划。
            if (shouldDelaySnapshot(snapshot)) {
                return SnapshotNotExistPlan.INSTANCE;
            }

            // first try to get overwrite changes
            // Paimon 的流读取默认是处理 APPEND 数据。如果遇到了 OVERWRITE 提交（通常是用户执行了 INSERT OVERWRITE 或通过增量更新产生了覆盖）：
            // 调用 handleOverwriteSnapshot 根据表类型产生撤回（Delete）和新增（Insert）消息。
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE) {
                SnapshotReader.Plan overwritePlan = handleOverwriteSnapshot(snapshot);
                if (overwritePlan != null) {
                    nextSnapshotId++;
                    if (overwritePlan.splits().isEmpty()) {
                        continue;
                    }
                    return overwritePlan;
                }
            }
            // 常规增量处理：FollowUp 扫描
            if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                SnapshotReader.Plan plan = followUpScanner.scan(snapshot, snapshotReader);
                currentWatermark = plan.watermark(); // 更新水位线
                nextSnapshotId++; // ID 消耗，指针向后移动
                if (plan.splits().isEmpty()) {
                    continue; // 如果扫描后发现全是压缩文件或被过滤了，没数据就处理下一个
                }
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    private boolean shouldDelaySnapshot(Snapshot snapshot) {
        if (scanDelayMillis == null) {
            return false;
        }

        long snapshotMills = System.currentTimeMillis() - scanDelayMillis;
        return snapshot.timeMillis() > snapshotMills;
    }

    @Nullable
    protected SnapshotReader.Plan handleOverwriteSnapshot(Snapshot snapshot) {
        if (supportStreamingReadOverwrite) {
            LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
            SnapshotReader.Plan overwritePlan =
                    followUpScanner.getOverwriteChangesPlan(snapshot, snapshotReader, !hasPk);
            currentWatermark = overwritePlan.watermark();
            return overwritePlan;
        }
        return null;
    }

    protected FollowUpScanner createFollowUpScanner() {
        switch (scanMode) {
            case COMPACT_BUCKET_TABLE:
                return new DeltaFollowUpScanner();
            case FILE_MONITOR:
                return new AllDeltaFollowUpScanner();
        }

        CoreOptions.ChangelogProducer changelogProducer = options.changelogProducer();
        FollowUpScanner followUpScanner;
        switch (changelogProducer) {
            case NONE:
                followUpScanner = new DeltaFollowUpScanner();
                break;
            case INPUT:
            case FULL_COMPACTION:
            case LOOKUP:
                followUpScanner = new ChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + changelogProducer.name());
        }
        return followUpScanner;
    }

    protected BoundedChecker createBoundedChecker() {
        Long boundedWatermark = options.scanBoundedWatermark();
        return boundedWatermark != null
                ? BoundedChecker.watermark(boundedWatermark)
                : BoundedChecker.neverEnd();
    }

    private Long getScanDelayMillis() {
        return options.streamingReadDelay() == null
                ? null
                : options.streamingReadDelay().toMillis();
    }

    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    @Nullable
    @Override
    public Long watermark() {
        return currentWatermark;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot) {
        if (nextSnapshotId != null && scanAllSnapshot) {
            startingScanner =
                    new StaticFromSnapshotStartingScanner(snapshotManager, nextSnapshotId);
            restore(null);
        } else {
            restore(nextSnapshotId);
        }
    }

    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        if (nextSnapshot == null) {
            return;
        }

        String consumerId = options.consumerId();
        if (consumerId != null) {
            snapshotReader.consumerManager().resetConsumer(consumerId, new Consumer(nextSnapshot));
        }
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}

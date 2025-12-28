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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.tag.TagPeriodHandler;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Get incremental data by {@link SnapshotReader#readIncrementalDiff}. */
// 专门用于执行**增量差异扫描（Incremental Diff Scan）**的启动器。
// 主要作用是：根据用户指定的范围（起点和终点），计算两个快照点之间的数据差异。
// 与一般的增量读取（只读新增）不同，IncrementalDiff 会对比两个状态，识别出哪些是新增（Insert），哪些是删除（Delete）。
// 它通常用于离线批处理中的差异计算，或者在指定两个 Tag/Snapshot/时间点 之间进行“补数”或“同步对比”。
public class IncrementalDiffStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalDiffStartingScanner.class);
    // 增量对比的起始快照。
    private final Snapshot start;
    // 增量对比的终止快照。
    private final Snapshot end;

    public IncrementalDiffStartingScanner(
            SnapshotManager snapshotManager, Snapshot start, Snapshot end) {
        super(snapshotManager);
        this.start = start;
        this.end = end;
        this.startingSnapshotId = start.id();
        // 如果表在 start 和 end 之间发生了 bucket 数量的扩缩容（Rescale），差异计算将无法准确进行，此方法会抛出异常。
        TimeTravelUtil.checkRescaleBucketForIncrementalDiffQuery(
                new SchemaManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch()),
                start,
                end);
    }
    // 执行真正的扫描逻辑。
    @Override
    public Result scan(SnapshotReader reader) {
        return StartingScanner.fromPlan(reader.withSnapshot(end).readIncrementalDiff(start));
    }
    // 根据指定的两个 Tag（标签） 名称生成扫描器。
    // 将 Tag 转换为对应的快照。如果 start 和 end 是同一个快照，则返回 EmptyResultStartingScanner（不产出数据）。
    public static StartingScanner betweenTags(
            Tag startTag,
            Tag endTag,
            SnapshotManager snapshotManager,
            Pair<String, String> incrementalBetween) {
        Snapshot start = startTag.trimToSnapshot();
        Snapshot end = endTag.trimToSnapshot();

        LOG.info(
                "{} start and end are parsed to tag with snapshot id {} to {}.",
                INCREMENTAL_BETWEEN.key(),
                start.id(),
                end.id());

        checkArgument(
                end.id() >= start.id(),
                "Tag end %s with snapshot id %s should be >= tag start %s with snapshot id %s",
                incrementalBetween.getRight(),
                end.id(),
                incrementalBetween.getLeft(),
                start.id());

        if (start.id() == end.id()) {
            return new EmptyResultStartingScanner(snapshotManager);
        }

        return new IncrementalDiffStartingScanner(snapshotManager, start, end);
    }
    // 直接根据两个 Snapshot ID 生成扫描器。
    public static StartingScanner betweenSnapshotIds(
            long startId, long endId, SnapshotManager snapshotManager) {
        Snapshot start = snapshotManager.snapshot(startId);
        Snapshot end = snapshotManager.snapshot(endId);
        return new IncrementalDiffStartingScanner(snapshotManager, start, end);
    }
    // 根据 时间戳范围 生成扫描器。
    // 通过 earlierOrEqualTimeMills 寻找对应时间点或最接近的快照。
    public static IncrementalDiffStartingScanner betweenTimestamps(
            long startTimestamp, long endTimestamp, SnapshotManager snapshotManager) {
        Snapshot startSnapshot = snapshotManager.earlierOrEqualTimeMills(startTimestamp);
        if (startSnapshot == null) {
            startSnapshot = snapshotManager.earliestSnapshot();
        }

        Snapshot endSnapshot = snapshotManager.earlierOrEqualTimeMills(endTimestamp);
        if (endSnapshot == null) {
            endSnapshot = snapshotManager.latestSnapshot();
        }

        return new IncrementalDiffStartingScanner(snapshotManager, startSnapshot, endSnapshot);
    }
    // 这是一个高级功能，用于自动寻找前一个周期的 Tag 并对比。
    // 用户指定一个结束 Tag（必须是自动创建的 Tag，如按天生成的）。
    // 程序通过 TagPeriodHandler 找到这个 Tag 对应的时间。
    // 应用场景：例如你配置了每天自动生成 Tag，现在想对比“今天”和“昨天”的数据差异，只需传入今天的 Tag 名称，它会自动帮你找到昨天的 Tag。
    public static AbstractStartingScanner toEndAutoTag(
            SnapshotManager snapshotManager, String endTagName, CoreOptions options) {
        TagPeriodHandler periodHandler = TagPeriodHandler.create(options);
        checkArgument(
                periodHandler.isAutoTag(endTagName),
                "Specified tag '%s' is not an auto-created tag.",
                endTagName);

        TagManager tagManager =
                new TagManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());

        Optional<Tag> endTag = tagManager.get(endTagName);
        if (!endTag.isPresent()) {
            LOG.info("Tag {} doesn't exist.", endTagName);
            return new EmptyResultStartingScanner(snapshotManager);
        }
        Snapshot end = endTag.get().trimToSnapshot();

        LocalDateTime endTagTime = periodHandler.tagToTime(endTagName);

        List<Pair<Tag, LocalDateTime>> previousTags =
                tagManager.tagObjects().stream()
                        .filter(p -> periodHandler.isAutoTag(p.getRight()))
                        .map(p -> Pair.of(p.getLeft(), periodHandler.tagToTime(p.getRight())))
                        .filter(p -> p.getRight().isBefore(endTagTime))
                        .sorted((tag1, tag2) -> tag2.getRight().compareTo(tag1.getRight()))
                        .collect(Collectors.toList());

        if (previousTags.isEmpty()) {
            LOG.info("Didn't found earlier tags for {}.", endTagName);
            return new EmptyResultStartingScanner(snapshotManager);
        }
        LOG.info("Found start tag {} .", periodHandler.timeToTag(previousTags.get(0).getRight()));
        Snapshot start = previousTags.get(0).getLeft().trimToSnapshot();

        return new IncrementalDiffStartingScanner(snapshotManager, start, end);
    }
}

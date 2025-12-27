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

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Read splits from specified {@link Snapshot} with given configuration. */
// 处于承上启下位置的核心接口
// 负责将静态的元数据（Snapshot、Manifest）转化为计算引擎（Flink/Spark）可以消费的动态任务单元（Splits）。
// SnapshotReader 的本质是一个**“分片规划器” (Split Planner)**。
// 元数据解析：读取特定的快照（Snapshot）文件，并进一步解析其关联的清单（Manifest）列表。
// 数据过滤与下推：在元数据层面应用分区过滤、Bucket 过滤和 LSM 层级过滤，减少不必要的文件扫描。
// 任务切分 (Planning)：根据用户配置（如批读取、增量读取、流读取）和数据分布（Sections/SortedRuns），将物理文件包装成 Split 对象。
public interface SnapshotReader {

    @Nullable
    Integer parallelism();
    // 获取管理快照和变更日志的管理器。
    SnapshotManager snapshotManager();

    ChangelogManager changelogManager();
    // 获取清单读取器，用于真正读取底层的 manifest-xxx 文件
    ManifestsReader manifestsReader();
    // 这是一个底层的辅助方法，
    // 用于读取特定的清单文件并返回其中的文件条目（ManifestEntry）
    List<ManifestEntry> readManifest(ManifestFileMeta manifest);

    ConsumerManager consumerManager();

    SplitGenerator splitGenerator();
    // 获取路径工厂，
    // 用于定位数据文件所在的物理路径。
    FileStorePathFactory pathFactory();
    // 指定要读取哪一个版本的快照数据
    SnapshotReader withSnapshot(long snapshotId);

    SnapshotReader withSnapshot(Snapshot snapshot);
    // 设置主键或字段过滤条件（Data Filter）
    SnapshotReader withFilter(Predicate predicate);
    // 设置分区过滤条件，支持多种参数（Spec、Predicate、Row列表等）。
    // 这是提升性能的关键，可以直接跳过整个分区目录。
    SnapshotReader withPartitionFilter(Map<String, String> partitionSpec);

    SnapshotReader withPartitionFilter(Predicate predicate);

    SnapshotReader withPartitionFilter(List<BinaryRow> partitions);

    SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate);

    SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions);
    // 设置扫描模式（如 ALL 全量, CHANGELOG 变更流等）
    SnapshotReader withMode(ScanMode scanMode);
    // 针对 LSM 树的特定层级进行过滤。
    SnapshotReader withLevel(int level);

    SnapshotReader withLevelFilter(Filter<Integer> levelFilter);

    SnapshotReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter);

    SnapshotReader enableValueFilter();

    SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter);
    // 限制只读取特定的桶，常用于计算引擎的并行度优化。
    SnapshotReader withBucket(int bucket);

    SnapshotReader onlyReadRealBuckets();
    // 限制只读取特定的桶，常用于计算引擎的并行度优化。
    SnapshotReader withBucketFilter(Filter<Integer> bucketFilter);

    SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter);
    // 是否在读取时丢弃列统计信息（Stats），丢弃可以减少内存占用，但会失去某些过滤优化。
    SnapshotReader dropStats();

    SnapshotReader keepStats();
    // 用于在多个并行子任务之间划分元数据扫描任务。
    SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    SnapshotReader withMetricRegistry(MetricRegistry registry);

    SnapshotReader withRowRanges(List<Range> rowRanges);

    SnapshotReader withReadType(RowType readType);

    SnapshotReader withLimit(int limit);

    /** Get splits plan from snapshot. */
    // 最重要的方法之一
    // 执行全量或当前配置模式下的分片规划，
    // 返回一个包含 Split 列表的 Plan。
    Plan read();

    /** Get splits plan from file changes. */
    // 读取快照中的增量变更文件（Add/Delete 记录）
    Plan readChanges();
    // 对比两个快照之间的差异，生成差异分片，常用于流式消费的起始位置处理。
    Plan readIncrementalDiff(Snapshot before);

    /** List partitions. */
    // 列出当前快照涉及的所有分区（BinaryRow 格式）
    List<BinaryRow> partitions();
    // 获取所有分区的统计详情
    List<PartitionEntry> partitionEntries();
    // 获取所有桶的统计详情
    List<BucketEntry> bucketEntries();
    // 获取一个迭代器，用于逐个遍历该快照包含的所有物理文件条目
    Iterator<ManifestEntry> readFileIterator();

    /** Result plan of this scan. */
    // Plan 是读取器产生的结果集，描述了这一次扫描的“执行蓝图”：
    interface Plan extends TableScan.Plan {
        // 获取快照关联的水位线，对流式任务至关重要。
        @Nullable
        Long watermark();

        /**
         * Snapshot id of this plan, return null if the table is empty or the manifest list is
         * specified.
         */
        // 记录该计划是基于哪个快照生成的。
        @Nullable
        Long snapshotId();

        /** Result splits. */
        // 最终生成的 Split 列表。每个 Split 对应一个或多个需要被读取的文件
        List<Split> splits();
        // 将 Split 强制转换为 Paimon 专用的 DataSplit 列表，方便处理 SortedRun 等逻辑。
        @SuppressWarnings({"unchecked", "rawtypes"})
        default List<DataSplit> dataSplits() {
            return (List) splits();
        }
    }
}

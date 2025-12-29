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

package org.apache.paimon.table;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An abstraction layer above {@link FileStore} to provide reading and writing of {@link
 * InternalRow}.
 */
// FileStoreTable 是最核心的接口之一。
// 它位于 FileStore（负责底层物理存储）和 Table（面向用户的逻辑表）之间。
// FileStoreTable 的核心作用是：作为衔接底层存储与高层 API 的桥梁。
// 封装物理存储逻辑：它屏蔽了底层 FileStore 中复杂的 LSM 树结构、Manifest 文件管理、快照（Snapshot）机制等细节。
// 提供行级读写：它继承了 DataTable 和 InnerTable，允许计算引擎（如 Flink, Spark）直接通过 InternalRow 格式进行读写操作。
// 状态与缓存管理：管理表级别的各种元数据缓存（如快照、统计信息、索引位图缓存），以优化读取性能。
public interface FileStoreTable extends DataTable {
    // 设置/获取 Manifest 文件的内存缓存。
    // Manifest 记录了数据文件的映射关系，缓存它可以大幅减少列出文件（Listing）的 IO 开销。
    void setManifestCache(SegmentsCache<Path> manifestCache);

    @Nullable
    SegmentsCache<Path> getManifestCache();
    // 设置快照缓存。快照记录了表在某一时刻的状态，频繁访问元数据时可直接命中缓存。
    void setSnapshotCache(Cache<Path, Snapshot> cache);
    // 设置统计信息缓存。用于存储文件的 Max/Min 值等，加速谓词下推的过滤。
    void setStatsCache(Cache<String, Statistics> cache);
    // 设置删除向量（Deletion Vector）元数据缓存。在 Merge-on-Read 模式下，缓存哪些行被删除的信息。
    void setDVMetaCache(DVMetaCache cache);
    // 从 TableSchema 中提取表的逻辑结构、分区字段和主键字段。
    @Override
    default RowType rowType() {
        return schema().logicalRowType();
    }

    @Override
    default List<String> partitionKeys() {
        return schema().partitionKeys();
    }

    @Override
    default List<String> primaryKeys() {
        return schema().primaryKeys();
    }
    // 定义桶（Bucket）的规范（桶数量、键）以及桶模式（固定桶、动态桶等）。
    default BucketSpec bucketSpec() {
        return new BucketSpec(bucketMode(), schema().bucketKeys(), schema().numBuckets());
    }

    default BucketMode bucketMode() {
        return store().bucketMode();
    }

    @Override
    default Map<String, String> options() {
        return schema().options();
    }

    @Override
    default Optional<String> comment() {
        return Optional.ofNullable(schema().comment());
    }
    // 获取当前表的完整 TableSchema。
    TableSchema schema();
    // 获取底层的物理 FileStore 实例。
    FileStore<?> store();
    // 获取表所属的目录环境（Catalog），包含权限、MetaStore 等信息。
    CatalogEnvironment catalogEnvironment();

    @Override
    FileStoreTable copy(Map<String, String> dynamicOptions);

    FileStoreTable copy(TableSchema newTableSchema);

    /** Doesn't change table schema even when there exists time travel scan options. */
    FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions);

    /** TODO: this method is weird, old options will overwrite new options. */
    // 将当前表的读写上下文切换到指定的分支（Branch）。
    FileStoreTable copyWithLatestSchema();

    @Override
    TableWriteImpl<?> newWrite(String commitUser);

    TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId);

    @Override
    TableCommitImpl newCommit(String commitUser);

    LocalTableQuery newLocalTableQuery();

    boolean supportStreamingReadOverwrite();

    RowKeyExtractor createRowKeyExtractor();

    /**
     * Get {@link DataTable} with branch identified by {@code branchName}. Note that this method
     * does not keep dynamic options in current table.
     */
    @Override
    FileStoreTable switchToBranch(String branchName);

    TagAutoManager newTagAutoManager();

    /** Purge all files in this table. */
    // 彻底清空表数据。
    default void purgeFiles() throws Exception {
        // 删除所有分支和标签（Tags）。
        // clear branches
        BranchManager branchManager = branchManager();
        branchManager.branches().forEach(branchManager::dropBranch);

        // clear tags
        TagManager tagManager = tagManager();
        tagManager.allTagNames().forEach(this::deleteTag);
        // 清理流式消费者位点（Consumers）
        // clear consumers
        ConsumerManager consumerManager = this.consumerManager();
        consumerManager.consumers().keySet().forEach(consumerManager::deleteConsumer);
        // 执行 truncateTable 物理截断数据。
        // truncate table
        try (BatchTableCommit commit = this.newBatchWriteBuilder().newCommit()) {
            commit.truncateTable();
        }

        // clear changelogs
        // 删除 Changelog 目录。
        ChangelogManager changelogManager = this.changelogManager();
        this.fileIO().delete(changelogManager.changelogDirectory(), true);

        // clear snapshots, keep only latest snapshot
        // 设置过期时间为 0，强制过期所有快照（只保留最后 1 个占位）
        this.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ZERO)
                                .build())
                .expire();

        // clear orphan files
        // ，扫描并删除物理磁盘上的孤立无用文件。
        LocalOrphanFilesClean clean = new LocalOrphanFilesClean(this, System.currentTimeMillis());
        clean.clean();
    }
}

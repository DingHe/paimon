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

package org.apache.paimon;

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexScanBuilder;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.partition.PartitionExpireStrategy;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;

/**
 * File store interface.
 *
 * @param <T> type of record to read and write.
 */
// 如果说 FileStoreTable 是面向用户的“逻辑层”，那么 FileStore 就是负责数据物理落地的**“存储引擎层”**。
// 物理存储网关：它屏蔽了具体的文件格式（如 ORC, Parquet）和物理布局，为上层提供统一的读（Read）、写（Write）、扫（Scan）和提交（Commit）能力。
// 组件工厂：它作为一个“中心工厂”，负责创建和维护管理数据生命周期所需的所有关键组件（如快照管理、清单文件处理、索引处理等）。
// 核心机制实现者：Paimon 最核心的 Snapshot（快照）机制 和 Manifest（清单）管理 都是通过 FileStore 的实现类来落地的。

public interface FileStore<T> {
    // 返回路径工厂。
    // 负责定义 Paimon 在文件系统中的目录结构（例如分区目录、Bucket 目录、元数据路径等）。
    FileStorePathFactory pathFactory();

    SnapshotManager snapshotManager();

    ChangelogManager changelogManager();
    // 返回分区字段的 RowType（类型信息）。
    RowType partitionType();
    // 返回分区计算器。
    // 负责从原始行数据中提取分区值并转换为底层的 BinaryRow。
    InternalRowPartitionComputer partitionComputer();
    // 获取表的配置项（CoreOptions），如压缩格式、合并引擎、分桶数等
    CoreOptions options();

    BucketMode bucketMode();

    FileStoreScan newScan();

    ManifestList.Factory manifestListFactory();

    ManifestFile.Factory manifestFileFactory();

    IndexManifestFile.Factory indexManifestFileFactory();

    IndexFileHandler newIndexFileHandler();

    StatsFileHandler newStatsFileHandler();

    SplitRead<T> newRead();

    FileStoreWrite<T> newWrite(String commitUser);

    FileStoreWrite<T> newWrite(String commitUser, @Nullable Integer writeId);

    FileStoreCommit newCommit(String commitUser, FileStoreTable table);

    SnapshotDeletion newSnapshotDeletion();

    ChangelogDeletion newChangelogDeletion();

    TagManager newTagManager();

    TagDeletion newTagDeletion();

    @Nullable
    PartitionExpire newPartitionExpire(String commitUser, FileStoreTable table);

    @Nullable
    PartitionExpire newPartitionExpire(
            String commitUser,
            FileStoreTable table,
            Duration expirationTime,
            Duration checkInterval,
            PartitionExpireStrategy expireStrategy);

    TagAutoManager newTagAutoManager(FileStoreTable table);

    ServiceManager newServiceManager();

    boolean mergeSchema(RowType rowType, boolean allowExplicitCast);

    List<TagCallback> createTagCallbacks(FileStoreTable table);

    void setManifestCache(SegmentsCache<Path> manifestCache);

    void setSnapshotCache(Cache<Path, Snapshot> cache);

    GlobalIndexScanBuilder newGlobalIndexScanBuilder();
}

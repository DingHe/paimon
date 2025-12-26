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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A table provides basic abstraction for table type and table scan and table read.
 *
 * @since 0.4.0
 */
// Table 接口是整个生态系统的核心抽象。
// 如果你把 Paimon 比作一个数据库，那么 Table 对象就是你操作数据的唯一入口，它定义了表是什么（元数据）以及你能对它做什么（操作）。
// Table 接口主要承担了三个维度的职责：
// 元数据持有者（Metadata Holder）：定义表的结构（列、类型）、分区键、主键以及配置选项。
// 状态管理（State Management）：提供对 Paimon 特有的快照（Snapshot）、标签（Tag）和分支（Branch）的操作能力。
// 这是 Paimon 实现数据版本回滚、时间旅行（Time Travel）和离线开发的基石。
// 读写入口（Read/Write Entry）：它是构建读取器（Reader）和写入器（Writer）的工厂。无论是 Flink、Spark 还是 Java 程序，都必须通过 Table 获取 ReadBuilder 或 WriteBuilder

@Public
public interface Table extends Serializable {

    // ================== Table Metadata =====================

    /** A name to identify this table. */
    // 返回表的名称。
    String name();

    /** Full name of the table, default is database.tableName. */
    // 返回表的完整限定名（通常是 database.table）
    default String fullName() {
        return name();
    }

    /**
     * UUID of the table, metastore can provide the true UUID of this table, default is the full
     * name.
     */
    // 返回表的唯一标识符。在元数据管理中，UUID 比名称更可靠，因为表可以重命名。
    default String uuid() {
        return fullName();
    }

    /** Returns the row type of this table. */
    // 返回表的列信息（RowType），包含列名、类型、注释等
    RowType rowType();

    /** Partition keys of this table. */
    // 返回作为分区键的列名列表
    List<String> partitionKeys();

    /** Primary keys of this table. */
    // 返回作为主键的列名列表
    List<String> primaryKeys();

    /** Options of this table. */
    // 返回表的配置参数（如 bucket 数量、压缩算法等）
    Map<String, String> options();

    /** Optional comment of this table. */
    // 返回表的注释说明
    Optional<String> comment();

    /** Optional statistics of this table. */
    // 回表的统计信息（如行数、文件大小等），主要用于查询优化器（CBO）。
    @Experimental
    Optional<Statistics> statistics();

    // ================= Table Operations ====================

    /** File io of this table. */
    // 获取底层文件系统的 IO 接口（支持 HDFS, S3, OSS 等）
    FileIO fileIO();

    /** Copy this table with adding dynamic options. */
    // 复制当前表对象，并注入动态配置。
    // 这在作业运行时临时调整参数（如提高写入并行度）非常有用
    Table copy(Map<String, String> dynamicOptions);

    /** Get the latest snapshot for this table, or empty if there are no snapshots. */
    // 获取最新的快照。
    // 快照代表了表在某一时刻的完整状态
    @Experimental
    Optional<Snapshot> latestSnapshot();

    /** Get the {@link Snapshot} from snapshot id. */
    // 根据特定的 ID 获取快照，用于实现时间旅行
    @Experimental
    Snapshot snapshot(long snapshotId);

    /** Reader to read manifest file meta from manifest list file. */
    // 读取快照关联的清单列表（Manifest List）
    @Experimental
    SimpleFileReader<ManifestFileMeta> manifestListReader();

    /** Reader to read manifest entry from manifest file. */
    // 读取具体的清单文件（Manifest File），里面记录了数据文件的增减。
    @Experimental
    SimpleFileReader<ManifestEntry> manifestFileReader();

    /** Reader to read index manifest entry from index manifest file. */
    // 读取索引文件的清单
    @Experimental
    SimpleFileReader<IndexManifestEntry> indexManifestFileReader();

    /** Rollback table's state to a specific snapshot. */
    // 将表的状态回退到某个快照 ID
    @Experimental
    void rollbackTo(long snapshotId);

    /** Create a tag from given snapshot. */
    // 将表状态回退到某个标签
    @Experimental
    void createTag(String tagName, long fromSnapshotId);
    // 创建标签，可指定过期时间
    @Experimental
    void createTag(String tagName, long fromSnapshotId, @Nullable Duration timeRetained);

    /** Create a tag from latest snapshot. */
    @Experimental
    void createTag(String tagName);

    @Experimental
    void createTag(String tagName, @Nullable Duration timeRetained);

    @Experimental
    void renameTag(String tagName, String targetTagName);

    /** Replace a tag with new snapshot id and new time retained. */
    @Experimental
    void replaceTag(String tagName, @Nullable Long fromSnapshotId, @Nullable Duration timeRetained);

    /** Delete a tag by name. */
    // 删除标签
    @Experimental
    void deleteTag(String tagName);

    /** Delete tags, tags are separated by commas. */
    @Experimental
    default void deleteTags(String tagStr) {
        String[] tagNames =
                Arrays.stream(tagStr.split(",")).map(String::trim).toArray(String[]::new);
        for (String tagName : tagNames) {
            deleteTag(tagName);
        }
    }

    /** Rollback table's state to a specific tag. */
    @Experimental
    void rollbackTo(String tagName);

    /** Create an empty branch. */
    // 从某个标签或起点创建新分支
    @Experimental
    void createBranch(String branchName);

    /** Create a branch from given tag. */
    @Experimental
    void createBranch(String branchName, String tagName);

    /** Delete a branch by branchName. */
    @Experimental
    void deleteBranch(String branchName);

    /** Delete branches, branches are separated by commas. */
    @Experimental
    default void deleteBranches(String branchNames) {
        for (String branch : branchNames.split(",")) {
            deleteBranch(branch);
        }
    }

    /** Merge a branch to main branch. */
    @Experimental
    void fastForward(String branchName);

    /** Manually expire snapshots, parameters can be controlled independently of table options. */
    // 创建一个快照过期清理器，防止元数据无限增长
    @Experimental
    ExpireSnapshots newExpireSnapshots();
    // 创建变更日志（Changelog）的过期清理器
    @Experimental
    ExpireSnapshots newExpireChangelog();

    // =============== Read & Write Operations ==================

    /** Returns a new read builder. */
    // 产生一个读取构建器。
    // 你可以通过它设置过滤条件（Filter）、投影（Projection，只读某些列），最终生成 TableScan 和 TableRead
    ReadBuilder newReadBuilder();

    /** Returns a new batch write builder. */
    // 产生一个批式写入构建器。
    // 用于离线处理，优化了大吞吐量的写入。
    BatchWriteBuilder newBatchWriteBuilder();

    /** Returns a new stream write builder. */
    // 产生一个流式写入构建器。
    // 这是 Paimon 最常用的场景，用于 Flink 流式写入，支持两阶段提交（2PC）
    StreamWriteBuilder newStreamWriteBuilder();
}

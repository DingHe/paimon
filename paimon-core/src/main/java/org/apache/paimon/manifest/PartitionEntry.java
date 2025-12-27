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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.manifest.FileKind.ADD;
import static org.apache.paimon.manifest.FileKind.DELETE;

/** Entry representing a partition. */
// 描述分区级别元数据统计信息的核心类
// 在 Paimon 中，一个表由许多分区组成，每个分区下又包含大量的清单文件（Manifests）和数据文件。如果每次查询都要扫描所有文件来获取表的规模，效率会极低。
// PartitionEntry 记录了一个分区当前的“健康状况”和“物理规模”，包括数据量、文件数和最后更新时间。它主要用于
// 元数据管理：在快照（Snapshot）中维护每个分区的聚合统计信息。
// 查询优化：帮助优化器了解各分区的数据分布，进行代价评估。
// 分区清理：通过 lastFileCreationTime 判断分区的生命周期。
@Public
public class PartitionEntry {
    // 分区的标识符。存储分区的实际值（二进制格式）。
    private final BinaryRow partition;
    // 记录总数。该分区内包含的行（Row）总数。
    private final long recordCount;
    // 物理大小。
    // 该分区下所有数据文件的总字节数。
    private final long fileSizeInBytes;
    // 文件数量。该分区包含的数据文件个数。
    private final long fileCount;
    // 最后修改时间。记录该分区中最新文件的创建时间戳（毫秒）。
    private final long lastFileCreationTime;

    public PartitionEntry(
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime) {
        this.partition = partition;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
    }

    public BinaryRow partition() {
        return partition;
    }

    public long recordCount() {
        return recordCount;
    }

    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    public long fileCount() {
        return fileCount;
    }

    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }
    // 将另一个 PartitionEntry 合并到当前条目中。
    // 它通过简单的加法（计数和大小）以及取最大值（最后创建时间）来生成一个新的条目。
    public PartitionEntry merge(PartitionEntry entry) {
        return new PartitionEntry(
                partition,
                recordCount + entry.recordCount,
                fileSizeInBytes + entry.fileSizeInBytes,
                fileCount + entry.fileCount,
                Math.max(lastFileCreationTime, entry.lastFileCreationTime));
    }
    // 将内部的二进制 BinaryRow 转换为用户可读的 Partition 对象
    public Partition toPartition(InternalRowPartitionComputer computer) {
        return new Partition(
                computer.generatePartValues(partition),
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                false);
    }
    // 转换为专门用于统计目的的 PartitionStatistics 对象。
    public PartitionStatistics toPartitionStatistics(InternalRowPartitionComputer computer) {
        return new PartitionStatistics(
                computer.generatePartValues(partition),
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime);
    }
    // 最核心的逻辑之一。根据文件的操作类型（ADD 或 DELETE）来计算权重：
    //如果是 ADD：增加记录数、文件大小和文件数。
    //如果是 DELETE：减去对应的记录数、文件大小和文件数。
    //这实现了增量更新统计信息的功能。
    public static PartitionEntry fromManifestEntry(ManifestEntry entry) {
        return fromDataFile(entry.partition(), entry.kind(), entry.file());
    }

    public static PartitionEntry fromDataFile(
            BinaryRow partition, FileKind kind, DataFileMeta file) {
        long recordCount = file.rowCount();
        long fileSizeInBytes = file.fileSize();
        long fileCount = 1;
        if (kind == DELETE) {
            recordCount = -recordCount;
            fileSizeInBytes = -fileSizeInBytes;
            fileCount = -fileCount;
        }
        return new PartitionEntry(
                partition, recordCount, fileSizeInBytes, fileCount, file.creationTimeEpochMillis());
    }
    // 将一组清单条目按分区进行分组汇总
    public static Collection<PartitionEntry> merge(Collection<ManifestEntry> fileEntries) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (ManifestEntry entry : fileEntries) {
            PartitionEntry partitionEntry = fromManifestEntry(entry);
            partitions.compute(
                    entry.partition(),
                    (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
        }
        return partitions.values();
    }
    // 在扫描（Scan）过程中，将查询分片（Split）的信息汇总。注意这里会忽略 DELETE 类型的文件，因为读取代价较高。
    public static Collection<PartitionEntry> mergeSplits(Collection<DataSplit> splits) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (DataSplit split : splits) {
            BinaryRow partition = split.partition();
            for (DataFileMeta file : split.dataFiles()) {
                PartitionEntry partitionEntry = fromDataFile(partition, ADD, file);
                partitions.compute(
                        partition,
                        (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
            }

            // Ignore before files, because we don't know how to merge them
            // Ignore deletion files, because it is costly to read from it
        }
        return partitions.values();
    }

    public static void merge(Collection<PartitionEntry> from, Map<BinaryRow, PartitionEntry> to) {
        for (PartitionEntry entry : from) {
            to.compute(entry.partition(), (part, old) -> old == null ? entry : old.merge(entry));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionEntry that = (PartitionEntry) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
    }
}

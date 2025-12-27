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
import org.apache.paimon.utils.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Entry representing a bucket. */
// BucketEntry 类是用于描述桶（Bucket）级别元数据统计信息的类。它是比 PartitionEntry 更细粒度的统计单元。
// 在 Paimon 的存储架构中，一个分区（Partition）通常被划分为多个桶（Bucket）。BucketEntry 记录了特定分区下某个特定桶的汇总信息。
// 精细化管理：Paimon 的数据读写和 Compaction 主要是以 Bucket 为最小单位进行的。了解每个 Bucket 的数据量和文件数，有助于触发更合理的 Compaction 策略。
// 动态桶（Dynamic Bucket）支持：在动态桶模式下，系统需要监控每个桶的大小，以便决定是否需要新增桶。
// 查询裁剪优化：虽然分区裁剪最常用，但在某些索引或过滤场景下，Bucket 级别的统计信息可以帮助更精确地估算数据分布。
@Public
public class BucketEntry {

    private final BinaryRow partition;
    private final int bucket;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final long fileCount;
    private final long lastFileCreationTime;

    public BucketEntry(
            BinaryRow partition,
            int bucket,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime) {
        this.partition = partition;
        this.bucket = bucket;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
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

    public BucketEntry merge(BucketEntry entry) {
        return new BucketEntry(
                partition,
                bucket,
                recordCount + entry.recordCount,
                fileSizeInBytes + entry.fileSizeInBytes,
                fileCount + entry.fileCount,
                Math.max(lastFileCreationTime, entry.lastFileCreationTime));
    }

    public static BucketEntry fromManifestEntry(ManifestEntry entry) {
        PartitionEntry partitionEntry = PartitionEntry.fromManifestEntry(entry);
        return new BucketEntry(
                partitionEntry.partition(),
                entry.bucket(),
                partitionEntry.recordCount(),
                partitionEntry.fileSizeInBytes(),
                partitionEntry.fileCount(),
                partitionEntry.lastFileCreationTime());
    }

    public static Collection<BucketEntry> merge(Collection<ManifestEntry> fileEntries) {
        Map<Pair<BinaryRow, Integer>, BucketEntry> buckets = new HashMap<>();
        for (ManifestEntry entry : fileEntries) {
            BucketEntry bucketEntry = fromManifestEntry(entry);
            buckets.compute(
                    Pair.of(entry.partition(), entry.bucket()),
                    (part, old) -> old == null ? bucketEntry : old.merge(bucketEntry));
        }
        return buckets.values();
    }

    public static void merge(
            Collection<BucketEntry> from, Map<Pair<BinaryRow, Integer>, BucketEntry> to) {
        for (BucketEntry entry : from) {
            to.compute(
                    Pair.of(entry.partition(), entry.bucket),
                    (part, old) -> old == null ? entry : old.merge(entry));
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
        BucketEntry that = (BucketEntry) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && bucket == that.bucket
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition, bucket, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
    }
}

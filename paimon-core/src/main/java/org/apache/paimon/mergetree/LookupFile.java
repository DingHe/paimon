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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.InternalRowPartitionComputer.partToSimpleString;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Lookup file for cache remote file to local. */
// 在 Apache Paimon 的 Merge Tree 存储体系中，LookupFile 是实现 Lookup 索引本地缓存的关键类。
// 当 Paimon 开启 lookup 功能时，为了避免频繁访问远程存储（如 HDFS/S3），系统会将远程的索引文件下载到本地磁盘，并封装成 LookupFile 进行高效查询。
// LookupFile 的主要作用是：管理本地化后的查找索引文件及其查询生命周期。
// 本地缓存管理：它代表了一个存在于本地磁盘的索引文件，关联了具体的 LookupStoreReader（读取器）。
// 点查代理：外部通过它提供的 get 方法进行主键查找，它内部调用 reader 获取结果。
// 统计与资源回收：记录该文件的查询命中率，并在文件过期或被缓存剔除时，负责关闭读取器并物理删除本地临时文件。
// 本地文件名生成：提供工具方法，确保不同分区、不同桶的同名文件在本地不会发生冲突。
public class LookupFile {

    private static final Logger LOG = LoggerFactory.getLogger(LookupFile.class);
    // 指向本地磁盘上缓存的物理文件。
    private final File localFile;
    // 该文件在 LSM-Tree 中所属的层级（Level），用于优先级判断。
    private final int level;
    // 记录创建该文件时的 Schema ID，确保数据解析的一致性。
    private final long schemaId;
    // 序列化版本号，用于兼容性校验。
    private final String serVersion;
    // 实际执行点查逻辑的读取器（如之前提到的 SortLookupStoreReader）。
    private final LookupStoreReader reader;
    // 一个回调钩子。当文件关闭并删除后执行，通常用于通知上层管理器更新状态。
    private final Runnable callback;
    // 分别记录“总查询次数”和“查询命中次数”，用于分析索引的有效性。
    private long requestCount;
    private long hitCount;
    // 防止文件关闭后被二次操作。
    private boolean isClosed = false;

    public LookupFile(
            File localFile,
            int level,
            long schemaId,
            String serVersion,
            LookupStoreReader reader,
            Runnable callback) {
        this.localFile = localFile;
        this.level = level;
        this.schemaId = schemaId;
        this.serVersion = serVersion;
        this.reader = reader;
        this.callback = callback;
    }

    public File localFile() {
        return localFile;
    }

    public long schemaId() {
        return schemaId;
    }

    public String serVersion() {
        return serVersion;
    }

    @Nullable
    public byte[] get(byte[] key) throws IOException {
        checkArgument(!isClosed);
        requestCount++;
        byte[] res = reader.lookup(key);
        if (res != null) {
            hitCount++;
        }
        return res;
    }

    public int level() {
        return level;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void close(RemovalCause cause) throws IOException {
        reader.close();
        isClosed = true;
        callback.run();
        LOG.info(
                "Delete Lookup file {} due to {}. Access stats: requestCount={}, hitCount={}, size={}KB",
                localFile.getName(),
                cause,
                requestCount,
                hitCount,
                localFile.length() >> 10);
        FileIOUtils.deleteFileOrDirectory(localFile);
    }

    // ==================== Cache for Local File ======================

    public static Cache<String, LookupFile> createCache(
            Duration fileRetention, MemorySize maxDiskSize) {
        return Caffeine.newBuilder()
                .expireAfterAccess(fileRetention)
                .maximumWeight(maxDiskSize.getKibiBytes())
                .weigher(LookupFile::fileWeigh)
                .removalListener(LookupFile::removalCallback)
                .executor(Runnable::run)
                .build();
    }

    private static int fileWeigh(String file, LookupFile lookupFile) {
        return fileKibiBytes(lookupFile.localFile);
    }

    private static void removalCallback(String file, LookupFile lookupFile, RemovalCause cause) {
        if (lookupFile != null) {
            try {
                lookupFile.close(cause);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static String localFilePrefix(
            RowType partitionType, BinaryRow partition, int bucket, String remoteFileName) {
        if (partition.getFieldCount() == 0) {
            return String.format("%s-%s", bucket, remoteFileName);
        } else {
            String partitionString = partToSimpleString(partitionType, partition, "-", 20);
            return String.format("%s-%s-%s", partitionString, bucket, remoteFileName);
        }
    }
}

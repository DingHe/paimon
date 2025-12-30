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

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.RemoteFileDownloader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Provide lookup by key. */
// LookupLevels 是实现 Lookup 索引 的核心组件。它通过在本地构建高效的磁盘索引文件（通常是 SST 格式），为 LSM-Tree 提供了快速的点查（Point Lookup）能力，
// 这对于产生 Changelog（lookup 生产模式）和去重引擎至关重要
// LookupLevels 的主要任务是：将数据文件（.data）映射为可快速点查的索引文件（.sst/.lookup）并提供查询接口。
// 由于原始的数据文件（如 ORC/Parquet）适合扫描但不适合随机点查，
// LookupLevels 会在本地磁盘创建一种类似 RocksDB 的 SST 结构。
// 当需要根据 Key 查找数据时，它会按照层级（从 L0 到最高层）依次检索这些索引文件，
// 并利用 Bloom Filter 和 Caffeine Cache 来优化性能。


public class LookupLevels<T> implements Levels.DropFileCallback, Closeable {

    public static final String REMOTE_LOOKUP_FILE_SUFFIX = ".lookup";

    private final Function<Long, RowType> schemaFunction;
    private final long currentSchemaId;
    // 维护 LSM-Tree 的层级结构信息。
    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    // 用于将 InternalRow 格式的 Key 序列化为紧凑的字节数组。
    private final RowCompactedSerializer keySerializer;
    // 逻辑处理器工厂，定义了如何处理存储在索引中的 Value。
    private final PersistProcessor.Factory<T> processorFactory;
    private final LookupSerializerFactory serializerFactory;
    // 用于读取原始数据文件（DataFile）的读取器工厂。
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    // 定义本地索引文件的存放路径（通常在本地磁盘的临时目录）
    private final Function<String, File> localFileFactory;
    // 用于创建底层的 KV 存储（如 RocksDB SST 格式）的 Reader 和 Writer。
    private final LookupStoreFactory lookupStoreFactory;
    // Bloom Filter 生成器，根据文件行数配置合适的误判率。
    private final Function<Long, BloomFilter.Builder> bfGenerator;
    // 本地索引文件缓存（基于 Caffeine），避免重复打开文件句柄。
    private final Cache<String, LookupFile> lookupFileCache;
    private final Set<String> ownCachedFiles;
    // 缓存不同 Schema ID 和序列化版本对应的处理器。
    private final Map<Pair<Long, String>, PersistProcessor<T>> schemaIdAndSerVersionToProcessors;
    // （可选）从远程分布式存储（如 HDFS/S3）下载预生成的索引文件。
    @Nullable private RemoteFileDownloader remoteFileDownloader;

    public LookupLevels(
            Function<Long, RowType> schemaFunction,
            long currentSchemaId,
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            PersistProcessor.Factory<T> processorFactory,
            LookupSerializerFactory serializerFactory,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Function<String, File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Function<Long, BloomFilter.Builder> bfGenerator,
            Cache<String, LookupFile> lookupFileCache) {
        this.schemaFunction = schemaFunction;
        this.currentSchemaId = currentSchemaId;
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.processorFactory = processorFactory;
        this.serializerFactory = serializerFactory;
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.bfGenerator = bfGenerator;
        this.lookupFileCache = lookupFileCache;
        this.ownCachedFiles = new HashSet<>();
        this.schemaIdAndSerVersionToProcessors = new ConcurrentHashMap<>();
        levels.addDropFileCallback(this);
    }

    public void setRemoteFileDownloader(@Nullable RemoteFileDownloader remoteFileDownloader) {
        this.remoteFileDownloader = remoteFileDownloader;
    }

    public Levels getLevels() {
        return levels;
    }

    @VisibleForTesting
    Cache<String, LookupFile> lookupFiles() {
        return lookupFileCache;
    }

    @VisibleForTesting
    Set<String> cachedFiles() {
        return ownCachedFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        lookupFileCache.invalidate(file);
    }

    @Nullable
    public T lookup(InternalRow key, int startLevel) throws IOException {
        return LookupUtils.lookup(levels, key, startLevel, this::lookup, this::lookupLevel0);
    }

    @Nullable
    private T lookupLevel0(InternalRow key, TreeSet<DataFileMeta> level0) throws IOException {
        return LookupUtils.lookupLevel0(keyComparator, key, level0, this::lookup);
    }

    @Nullable
    private T lookup(InternalRow key, SortedRun level) throws IOException {
        return LookupUtils.lookup(keyComparator, key, level, this::lookup);
    }
    // lookup 方法是执行点查（Point Lookup）的核心逻辑。它负责协调本地磁盘缓存、索引查询以及结果的反序列化。
    @Nullable
    private T lookup(InternalRow key, DataFileMeta file) throws IOException {
        // 据数据文件的名称，从 lookupFileCache（基于 Caffeine 的本地内存+磁盘缓存）中尝试获取已加载的 LookupFile 对象。
        LookupFile lookupFile = lookupFileCache.getIfPresent(file.fileName());

        boolean newCreatedLookupFile = false;
        // 如果缓存中没有该文件的索引，则调用 createLookupFile。
        if (lookupFile == null) {
            lookupFile = createLookupFile(file); // 内部执行下载或构建
            newCreatedLookupFile = true;
        }

        byte[] valueBytes;
        try {
            byte[] keyBytes = keySerializer.serializeToBytes(key);
            // 执行点查逻辑
            valueBytes = lookupFile.get(keyBytes);
        } finally {
            if (newCreatedLookupFile) {
                addLocalFile(file, lookupFile);
            }
        }
        if (valueBytes == null) {
            return null;
        }

        return getOrCreateProcessor(lookupFile.schemaId(), lookupFile.serVersion())
                .readFromDisk(key, lookupFile.level(), valueBytes, file.fileName());
    }

    private PersistProcessor<T> getOrCreateProcessor(long schemaId, String serVersion) {
        return schemaIdAndSerVersionToProcessors.computeIfAbsent(
                Pair.of(schemaId, serVersion),
                id -> {
                    RowType fileSchema =
                            schemaId == currentSchemaId ? null : schemaFunction.apply(schemaId);
                    return processorFactory.create(serVersion, serializerFactory, fileSchema);
                });
    }
    // 负责将远程的底层数据文件（Data File）转化为本地可高效查询的索引文件。这个过程包含远程文件下载或实时索引构建。
    public LookupFile createLookupFile(DataFileMeta file) throws IOException {
        // 根据远程文件的名称，通过 localFileFactory 生成一个本地磁盘上的文件路径对象。
        File localFile = localFileFactory.apply(file.fileName());
        // 在本地文件系统中物理创建该文件。
        if (!localFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }

        long schemaId = this.currentSchemaId;
        String fileSerVersion = serializerFactory.version();
        // 尝试从远程存储（如 HDFS/S3）直接下载预生成的 SST 索引文件。
        // 关键点：Paimon 会在后台线程或 Compaction 时预先生成索引（.sst）。如果远程已经有了，直接下载是最快的方案。
        Optional<String> downloadSerVersion = tryToDownloadRemoteSst(file, localFile);
        if (downloadSerVersion.isPresent()) {
            // use schema id from remote file
            schemaId = file.schemaId();
            fileSerVersion = downloadSerVersion.get();
        } else {
            // 如果远程没有预生成的索引，则调用此方法读取原始数据文件（Data File），在本地实时构建出一套 SST 索引并写入 localFile。
            createSstFileFromDataFile(file, localFile);
        }

        ownCachedFiles.add(file.fileName());
        return new LookupFile(
                localFile,
                file.level(),
                schemaId,
                fileSerVersion,
                lookupStoreFactory.createReader(localFile),
                () -> ownCachedFiles.remove(file.fileName()));
    }
    // 检查远程存储（如 HDFS）中是否已经存在预先生成的索引文件（SST），如果存在且兼容，则直接下载到本地，避免在本地消耗 CPU 重新构建索引。
    private Optional<String> tryToDownloadRemoteSst(DataFileMeta file, File localFile) {
        // 判断是否配置了远程文件下载器。
        // 如果 Paimon 运行在纯本地模式或者没有配置 lookup.remote.downloader，则直接返回空，表示无法从远程下载。
        if (remoteFileDownloader == null) {
            return Optional.empty();
        }
        // 根据当前的数据文件（file）去寻找对应的远程 SST 描述信息。
        Optional<RemoteSstFile> remoteSstFile = remoteSst(file);
        if (!remoteSstFile.isPresent()) {
            return Optional.empty();
        }

        RemoteSstFile remoteSst = remoteSstFile.get();

        // validate schema matched, no exception here
        // 确保本地代码能够解析远程索引。
        try {
            getOrCreateProcessor(file.schemaId(), remoteSst.serVersion);
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
        // 调用底层文件系统的 API 执行具体的下载动作。
        boolean success =
                remoteFileDownloader.tryToDownload(file, remoteSst.sstFileName, localFile);
        if (!success) {
            return Optional.empty();
        }

        return Optional.of(remoteSst.serVersion);
    }

    public void addLocalFile(DataFileMeta file, LookupFile lookupFile) {
        lookupFileCache.put(file.fileName(), lookupFile);
    }
    // 通过扫描原始的数据文件（Data File，如 ORC/Parquet），在本地内存中实时提取 Key-Value 记录，并将其构建成一个新的本地 SST 索引文件。
    private void createSstFileFromDataFile(DataFileMeta file, File localFile) throws IOException {
        // 创建一个 SST 格式的写入器
        try (LookupStoreWriter kvWriter =
                        lookupStoreFactory.createWriter(
                                localFile, bfGenerator.apply(file.rowCount()));
                RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            // 获取一个持久化处理器。
            // 该处理器负责将读取到的 KeyValue 对象转换（序列化）为最终存储在索引中的 byte[] 格式。
            PersistProcessor<T> processor =
                    getOrCreateProcessor(currentSchemaId, serializerFactory.version());
            KeyValue kv;
            // 带位置信息的写入 (processor.withPosition())
            if (processor.withPosition()) {
                FileRecordIterator<KeyValue> batch;
                while ((batch = (FileRecordIterator<KeyValue>) reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes = processor.persistToDisk(kv, batch.returnedPosition());
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            } else {
                // 不带位置信息的常规写入
                RecordReader.RecordIterator<KeyValue> batch;
                while ((batch = reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes = processor.persistToDisk(kv);
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        }
    }

    public Optional<RemoteSstFile> remoteSst(DataFileMeta file) {
        Optional<String> sstFile =
                file.extraFiles().stream()
                        .filter(f -> f.endsWith(REMOTE_LOOKUP_FILE_SUFFIX))
                        .findFirst();
        if (!sstFile.isPresent()) {
            return Optional.empty();
        }

        String sstFileName = sstFile.get();
        String[] split = sstFileName.split("\\.");
        if (split.length < 3) {
            return Optional.empty();
        }

        String processorId = split[split.length - 3];
        if (!processorFactory.identifier().equals(processorId)) {
            return Optional.empty();
        }

        String serVersion = split[split.length - 2];
        return Optional.of(new RemoteSstFile(sstFileName, serVersion));
    }

    public String newRemoteSst(DataFileMeta file, long length) {
        return file.fileName()
                + "."
                + length
                + "."
                + processorFactory.identifier()
                + "."
                + serializerFactory.version()
                + REMOTE_LOOKUP_FILE_SUFFIX;
    }

    @Override
    public void close() throws IOException {
        Set<String> toClean = new HashSet<>(ownCachedFiles);
        for (String cachedFile : toClean) {
            lookupFileCache.invalidate(cachedFile);
        }
    }

    /** Remote sst file with serVersion. */
    public static class RemoteSstFile {

        private final String sstFileName;
        private final String serVersion;

        private RemoteSstFile(String sstFileName, String serVersion) {
            this.sstFileName = sstFileName;
            this.serVersion = serVersion;
        }
    }
}

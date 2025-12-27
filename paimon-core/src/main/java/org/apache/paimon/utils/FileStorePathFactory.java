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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexInDataFileDirPathFactory;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.ChainReadContext;
import org.apache.paimon.io.ChainReadDataFilePathFactory;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Factory which produces {@link Path}s for manifest files. */
// FileStorePathFactory 是一个极其关键的底层工具类。它负责定义和管理 Paimon 表在文件系统（如 HDFS、S3、OSS）中的目录结构和文件名生成规则
// 在 Paimon 的存储架构中，数据被分为多种类型：数据文件（Data Files）、清单文件（Manifests）、清单列表（Manifest Lists）、索引（Index）和统计信息（Statistics）。这些文件分布在不同的分层目录中（例如按分区 partition 和桶 bucket 划分）。
// 该类的核心作用包括：
// 目录层级规范化：确定 manifest/、index/、statistics/ 等特殊目录的根路径。
// 文件名唯一性保证：通过 UUID 和原子计数器，确保生成的每一个文件名在全局范围内是唯一的，避免写冲突。
// 分区与桶路径解析：根据传入的 BinaryRow（分区数据）和 bucket 编号，计算出物理存储路径。
// [Table Root]
//├── manifest/
//│   ├── manifest-<uuid>-0
//│   └── manifest-list-<uuid>-0
//├── index/ (如果不是局部索引)
//│   └── index-<uuid>-0
//├── statistics/
//│   └── stat-<uuid>-0
//└── dt=20231027/ (分区目录)
//    ├── bucket-0/ (桶目录)
//    │   ├── data-<uuid>-0.orc
//    │   └── index-<uuid>-0 (如果是局部索引)
//    └── bucket-1/
@ThreadSafe
public class FileStorePathFactory {
    // 定义清单目录名为 manifest，文件前缀为 manifest-。
    public static final String MANIFEST_PATH = "manifest";
    public static final String MANIFEST_PREFIX = "manifest-";
    // 清单列表文件前缀 manifest-list-
    public static final String MANIFEST_LIST_PREFIX = "manifest-list-";
    public static final String INDEX_MANIFEST_PREFIX = "index-manifest-";
    // 索引目录名为 index，文件前缀为 index-
    public static final String INDEX_PATH = "index";
    public static final String INDEX_PREFIX = "index-";
    // 统计信息目录名为 statistics
    public static final String STATISTICS_PATH = "statistics";
    public static final String STATISTICS_PREFIX = "stat-";
    // 桶目录的前缀，通常是 bucket-
    public static final String BUCKET_PATH_PREFIX = "bucket-";

    // this is the table schema root path
    // 表的根路径。
    private final Path root;
    // 每一个工厂实例生成的唯一标识符，用于文件名中以区分不同的写入任务。
    private final String uuid;
    // 分区计算器。
    // 负责将 BinaryRow 格式的分区数据转换为字符串路径（如 dt=20231001/）
    private final InternalRowPartitionComputer partitionComputer;
    // 文件格式（如 orc, parquet, avro）
    private final String formatIdentifier;
    // 数据文件和变更日志文件的前缀
    private final String dataFilePrefix;
    private final String changelogFilePrefix;

    private final boolean fileSuffixIncludeCompression;
    private final String fileCompression;
    // 如果配置了特定的数据存储目录，则使用此属性，否则默认存放在根目录。
    @Nullable private final String dataFilePathDirectory;
    // 决定索引文件是放在全局 index/ 目录，还是放在数据文件所在的 bucket 目录内。
    private final boolean indexFileInDataFileDir;
    // 原子计数器（用于生成唯一文件名）
    // 确保在同一个 JVM 进程内，生成的文件编号不会重复。
    private final AtomicInteger manifestFileCount;
    private final AtomicInteger manifestListCount;
    private final AtomicInteger indexManifestCount;
    private final AtomicInteger indexFileCount;
    private final AtomicInteger statsFileCount;
    private final List<Path> externalPaths;

    public FileStorePathFactory(
            Path root,
            RowType partitionType,
            String defaultPartValue,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean legacyPartitionName,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable String dataFilePathDirectory,
            List<Path> externalPaths,
            boolean indexFileInDataFileDir) {
        this.root = root;
        this.dataFilePathDirectory = dataFilePathDirectory;
        this.indexFileInDataFileDir = indexFileInDataFileDir;
        this.uuid = UUID.randomUUID().toString();

        this.partitionComputer =
                getPartitionComputer(partitionType, defaultPartValue, legacyPartitionName);
        this.formatIdentifier = formatIdentifier;
        this.dataFilePrefix = dataFilePrefix;
        this.changelogFilePrefix = changelogFilePrefix;
        this.fileSuffixIncludeCompression = fileSuffixIncludeCompression;
        this.fileCompression = fileCompression;

        this.manifestFileCount = new AtomicInteger(0);
        this.manifestListCount = new AtomicInteger(0);
        this.indexManifestCount = new AtomicInteger(0);
        this.indexFileCount = new AtomicInteger(0);
        this.statsFileCount = new AtomicInteger(0);
        this.externalPaths = externalPaths;
    }

    public Path root() {
        return root;
    }

    public Path manifestPath() {
        return new Path(root, MANIFEST_PATH);
    }

    public Path indexPath() {
        return new Path(root, INDEX_PATH);
    }

    public Path statisticsPath() {
        return new Path(root, STATISTICS_PATH);
    }

    public Path dataFilePath() {
        if (dataFilePathDirectory != null) {
            return new Path(root, dataFilePathDirectory);
        }
        return root;
    }

    @VisibleForTesting
    public static InternalRowPartitionComputer getPartitionComputer(
            RowType partitionType, String defaultPartValue, boolean legacyPartitionName) {
        String[] partitionColumns = partitionType.getFieldNames().toArray(new String[0]);
        return new InternalRowPartitionComputer(
                defaultPartValue, partitionType, partitionColumns, legacyPartitionName);
    }

    public Path newManifestFile() {
        return toManifestFilePath(
                MANIFEST_PREFIX + uuid + "-" + manifestFileCount.getAndIncrement());
    }

    public Path newManifestList() {
        return toManifestListPath(
                MANIFEST_LIST_PREFIX + uuid + "-" + manifestListCount.getAndIncrement());
    }

    public Path toManifestFilePath(String manifestFileName) {
        return new Path(manifestPath(), manifestFileName);
    }

    public Path toManifestListPath(String manifestListName) {
        return new Path(manifestPath(), manifestListName);
    }
    // 非常重要。
    // 为特定的分区和桶创建一个 DataFilePathFactory。
    // 这个子工厂将负责具体的数据文件名（如 .orc 后缀的文件）生成。
    public DataFilePathFactory createDataFilePathFactory(BinaryRow partition, int bucket) {
        return new DataFilePathFactory(
                bucketPath(partition, bucket),
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                createExternalPathProvider(partition, bucket));
    }

    public ChainReadDataFilePathFactory createChainReadDataFilePathFactory(
            ChainReadContext chainReadContext) {
        if (externalPaths != null && !externalPaths.isEmpty()) {
            throw new IllegalArgumentException("Chain read does not support external path.");
        }
        return new ChainReadDataFilePathFactory(
                root,
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                null,
                chainReadContext);
    }

    public DataFilePathFactory createFormatTableDataFilePathFactory(
            BinaryRow partition, boolean onlyValue) {
        return new DataFilePathFactory(
                partitionPath(partition, onlyValue),
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                createExternalPartitionPathProvider(partition));
    }

    private ExternalPathProvider createExternalPartitionPathProvider(
            BinaryRow partition, boolean onlyValue) {
        if (externalPaths == null || externalPaths.isEmpty()) {
            return null;
        }

        return new ExternalPathProvider(externalPaths, partitionPath(partition, onlyValue));
    }

    private ExternalPathProvider createExternalPartitionPathProvider(BinaryRow partition) {
        if (externalPaths == null || externalPaths.isEmpty()) {
            return null;
        }

        return new ExternalPathProvider(externalPaths, partitionPath(partition));
    }

    private Path partitionPath(BinaryRow partition, boolean onlyValue) {
        Path relativeBucketPath = null;
        String partitionPath = getPartitionString(partition, onlyValue);
        if (!partitionPath.isEmpty()) {
            relativeBucketPath = new Path(partitionPath);
        }
        if (dataFilePathDirectory != null) {
            relativeBucketPath =
                    relativeBucketPath != null
                            ? new Path(dataFilePathDirectory, relativeBucketPath)
                            : new Path(dataFilePathDirectory);
        }
        return relativeBucketPath != null ? new Path(root, relativeBucketPath) : root;
    }

    public Path partitionPath(BinaryRow partition) {
        return partitionPath(partition, false);
    }

    @Nullable
    private ExternalPathProvider createExternalPathProvider(BinaryRow partition, int bucket) {
        if (externalPaths == null || externalPaths.isEmpty()) {
            return null;
        }

        return new ExternalPathProvider(externalPaths, relativeBucketPath(partition, bucket));
    }

    public List<Path> getExternalPaths() {
        return externalPaths;
    }

    public Path bucketPath(BinaryRow partition, int bucket) {
        return new Path(root, relativeBucketPath(partition, bucket));
    }

    public Path relativeBucketPath(BinaryRow partition, int bucket) {
        String bucketName = String.valueOf(bucket);
        if (bucket == BucketMode.POSTPONE_BUCKET) {
            bucketName = "postpone";
        }
        Path relativeBucketPath = new Path(BUCKET_PATH_PREFIX + bucketName);
        String partitionPath = getPartitionString(partition);
        if (!partitionPath.isEmpty()) {
            relativeBucketPath = new Path(partitionPath, relativeBucketPath);
        }
        if (dataFilePathDirectory != null) {
            relativeBucketPath = new Path(dataFilePathDirectory, relativeBucketPath);
        }
        return relativeBucketPath;
    }

    /** IMPORTANT: This method is NOT THREAD SAFE. */
    public String getPartitionString(BinaryRow partition) {
        return PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected.")));
    }

    public String getPartitionString(BinaryRow partition, boolean onlyValue) {
        return PartitionPathUtils.generatePartitionPathUtil(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected.")),
                onlyValue);
    }

    // @TODO, need to be changed
    public List<Path> getHierarchicalPartitionPath(BinaryRow partition) {
        return PartitionPathUtils.generateHierarchicalPartitionPaths(
                        partitionComputer.generatePartValues(
                                Preconditions.checkNotNull(
                                        partition,
                                        "Partition binary row is null. This is unexpected.")))
                .stream()
                .map(p -> new Path(root + "/" + p))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public String uuid() {
        return uuid;
    }

    public PathFactory manifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestFilePath(fileName);
            }
        };
    }

    public PathFactory manifestListFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestList();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestListPath(fileName);
            }
        };
    }

    public PathFactory indexManifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return toPath(
                        INDEX_MANIFEST_PREFIX + uuid + "-" + indexManifestCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(manifestPath(), fileName);
            }
        };
    }

    public IndexPathFactory indexFileFactory(BinaryRow partition, int bucket) {
        if (indexFileInDataFileDir) {
            DataFilePathFactory dataFilePathFactory = createDataFilePathFactory(partition, bucket);
            return new IndexInDataFileDirPathFactory(uuid, indexFileCount, dataFilePathFactory);
        } else {
            return globalIndexFileFactory();
        }
    }

    public IndexPathFactory globalIndexFileFactory() {
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(indexPath(), fileName);
            }

            @Override
            public Path newPath() {
                return toPath(INDEX_PREFIX + uuid + "-" + indexFileCount.getAndIncrement());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    public PathFactory statsFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return toPath(STATISTICS_PREFIX + uuid + "-" + statsFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(statisticsPath(), fileName);
            }
        };
    }

    public static Function<String, FileStorePathFactory> createFormatPathFactories(
            CoreOptions options,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory) {
        Map<String, FileStorePathFactory> map = new ConcurrentHashMap<>();
        return format -> map.computeIfAbsent(format, k -> formatPathFactory.apply(options, format));
    }
}

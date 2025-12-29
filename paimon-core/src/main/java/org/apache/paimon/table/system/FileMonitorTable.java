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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.CoreOptions.STREAM_SCAN_MODE;
import static org.apache.paimon.CoreOptions.StreamScanMode.FILE_MONITOR;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** A table to produce modified files for snapshots. */
// FileMonitorTable 是一个特殊的系统表（System Table）。它并不存储实际的用户数据，而是作为一种元数据观测视图存在。
// FileMonitorTable 的核心作用是：监控并产生快照（Snapshot）级别的文件变更信息。
// 通常情况下，Paimon 的表返回的是行数据。但 FileMonitorTable 返回的是**“文件的变化”**。当你查询这个表时，它会告诉你：
// 在某次快照中，哪些分区和桶里的哪些文件被删除了（Before Files），哪些新文件被增加了（Data Files）。
// 这在以下场景中非常有用：
//
//构建二级索引：外部系统需要知道哪些物理文件发生了变化，以便更新外部索引。
//
//审计与监控：监控存储层的文件布局演变和数据写入频率。
//
//异步处理：触发基于文件落地的下游离线处理任务。
@Experimental
public class FileMonitorTable implements DataTable, ReadonlyTable {

    private static final long serialVersionUID = 1L;
    // 被包装的原始物理表（主表）
    // 提供实际的快照管理、扫描和元数据读取能力。所有的监控逻辑都是基于这个主表的变更进行的。
    private final FileStoreTable wrapped;
    // 定义了该系统表的 Schema 结构。
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        new BigIntType(false), // 产生变更的快照 ID。
                        newBytesType(false), // 变更发生的分区。
                        new IntType(false), // 变更发生的桶 ID。
                        newBytesType(false), // 该次变更中被移除或覆盖的文件列表（序列化后的字节）。
                        newBytesType(false) // 该次变更中新增的文件列表（序列化后的字节）。
                    },
                    new String[] {
                        "_SNAPSHOT_ID", "_PARTITION", "_BUCKET", "_BEFORE_FILES", "_DATA_FILES"
                    });

    public FileMonitorTable(FileStoreTable wrapped) {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(STREAM_SCAN_MODE.key(), FILE_MONITOR.getValue());
        dynamicOptions.put(SCAN_BOUNDED_WATERMARK.key(), null);
        this.wrapped = wrapped.copy(dynamicOptions);
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return wrapped.changelogManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return wrapped.consumerManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return wrapped.schemaManager();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new FileMonitorTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public String name() {
        return "__internal_file_monitor_" + wrapped.location().getName();
    }

    @Override
    public RowType rowType() {
        return ROW_TYPE;
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return wrapped.newSnapshotReader();
    }

    @Override
    public DataTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return wrapped.newStreamScan();
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public InnerTableRead newRead() {
        return new BucketsRead();
    }

    @Override
    public FileMonitorTable copy(Map<String, String> dynamicOptions) {
        return new FileMonitorTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    public static RowType getRowType() {
        return ROW_TYPE;
    }

    private static class BucketsRead implements InnerTableRead {

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // filter is done by scan
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof DataSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            DataSplit dataSplit = (DataSplit) split;

            FileChange change =
                    new FileChange(
                            dataSplit.snapshotId(),
                            dataSplit.partition(),
                            dataSplit.bucket(),
                            dataSplit.beforeFiles(),
                            dataSplit.dataFiles());

            return new IteratorRecordReader<>(Collections.singletonList(toRow(change)).iterator());
        }
    }

    public static InternalRow toRow(FileChange change) throws IOException {
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        return GenericRow.of(
                change.snapshotId(),
                serializeBinaryRow(change.partition()),
                change.bucket(),
                fileSerializer.serializeList(change.beforeFiles()),
                fileSerializer.serializeList(change.dataFiles()));
    }

    public static FileChange toFileChange(InternalRow row) throws IOException {
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        return new FileChange(
                row.getLong(0),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                fileSerializer.deserializeList(row.getBinary(3)),
                fileSerializer.deserializeList(row.getBinary(4)));
    }

    /** Pojo to record of file change. */
    public static class FileChange {

        private final long snapshotId;
        private final BinaryRow partition;
        private final int bucket;
        private final List<DataFileMeta> beforeFiles;
        private final List<DataFileMeta> dataFiles;

        public FileChange(
                long snapshotId,
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> beforeFiles,
                List<DataFileMeta> dataFiles) {
            this.snapshotId = snapshotId;
            this.partition = partition;
            this.bucket = bucket;
            this.beforeFiles = beforeFiles;
            this.dataFiles = dataFiles;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public List<DataFileMeta> beforeFiles() {
            return beforeFiles;
        }

        public List<DataFileMeta> dataFiles() {
            return dataFiles;
        }

        @Override
        public String toString() {
            return "FileChange{"
                    + "snapshotId="
                    + snapshotId
                    + ", partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", beforeFiles="
                    + beforeFiles
                    + ", dataFiles="
                    + dataFiles
                    + '}';
        }
    }
}

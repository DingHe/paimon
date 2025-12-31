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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A {@link RecordWriter} to write records and generate {@link CompactIncrement}. */
// MergeTreeWriter 是最核心的写入组件。它实现了 LSM-Tree 的写入流程，负责将数据从内存（MemTable）持久化到磁盘，
// 并与 CompactManager 配合管理层级结构的平衡。
// MergeTreeWriter 的本质是一个 数据协调者，它的主要职责包括：
// 缓存写入：将输入的 KeyValue 记录暂存在内存的 WriteBuffer 中。
// 排序合并：当内存写满时，对内存中的数据按 Key 进行排序，并应用 MergeFunction（如去重、聚合）进行初步合并。
// 刷新落盘（Flush）：将内存数据写成 Level 0 的文件。
// 压缩调度：通过驱动 CompactManager，触发后台的异步合并任务，并收集合并产生的结果（增量文件）。
// 事务准备：为 Commit 阶段准备 CommitIncrement（包含新增数据文件、变更日志文件以及压缩后的元数据变化）。
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {
    // 内存写缓冲区是否允许在写满时溢写到磁盘。
    private final boolean writeBufferSpillable;
    // 允许溢写到磁盘的最大空间限制。
    private final MemorySize maxDiskSize;
    // 用于内存数据排序时的扇出度控制和压缩选项。
    private final int sortMaxFan;
    private final CompressOptions sortCompression;
    // 用于处理磁盘 IO 溢写的管理器。
    private final IOManager ioManager;
    // 定义 Key 和 Value 的行类型。
    private final RowType keyType;
    private final RowType valueType;
    // 负责管理后台的异步 Compaction 任务。
    private final CompactManager compactManager;
    // 定义 Key 比较逻辑。
    private final Comparator<InternalRow> keyComparator;
    // 定义当 Key 冲突时如何合并（例如保留最新、求和等）。
    private final MergeFunction<KeyValue> mergeFunction;
    // 用于创建数据文件和 Changelog 文件的工厂类。
    private final KeyValueFileWriterFactory writerFactory;
    private final boolean commitForceCompact;
    // Changelog 产生策略（如 INPUT 表示直接根据写入数据产生）。
    private final ChangelogProducer changelogProducer;
    @Nullable private final FieldsComparator userDefinedSeqComparator;
    // 当前写入周期新生成的 Level 0 数据文件。
    private final LinkedHashSet<DataFileMeta> newFiles;
    // 在本次写入中被逻辑删除的文件（通常用于覆盖写）。
    private final LinkedHashSet<DataFileMeta> deletedFiles;
    // 伴随新数据落盘产生的变更日志文件。
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    // 记录 Compaction 的变化情况（哪些旧文件被合并成了哪些新文件）。
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    private final LinkedHashSet<DataFileMeta> compactAfter;
    // 压缩过程中产生的变更日志。
    private final LinkedHashSet<DataFileMeta> compactChangelog;
    // 维护压缩过程中涉及的删除向量（Deletion Vector）文件。
    @Nullable private CompactDeletionFile compactDeletionFile;
    // 当前处理的数据序列号，用于实现数据的单调递增。
    private long newSequenceNumber;
    // 内存中的 MemTable。
    private WriteBuffer writeBuffer;

    public MergeTreeWriter(
            boolean writeBufferSpillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions sortCompression,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.maxDiskSize = maxDiskSize;
        this.sortMaxFan = sortMaxFan;
        this.sortCompression = sortCompression;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;
        this.userDefinedSeqComparator = userDefinedSeqComparator;

        this.newFiles = new LinkedHashSet<>();
        this.deletedFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
            updateCompactDeletionFile(increment.compactDeletionFile());
        }
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    public CompactManager compactManager() {
        return compactManager;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        userDefinedSeqComparator,
                        memoryPool,
                        writeBufferSpillable,
                        maxDiskSize,
                        sortMaxFan,
                        sortCompression,
                        ioManager);
    }
    // 负责将数据写入内存缓冲区（MemTable），并在缓冲区满时触发刷盘。
    @Override
    public void write(KeyValue kv) throws Exception {
        // 生成一个新的序列号（Sequence Number）
        long sequenceNumber = newSequenceNumber();
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        // 如果返回 false，通常意味着当前 MemTable 的空间已经达到了预设的阈值（通常由 write-buffer-size 参数控制），无法再容纳更多数据。
        if (!success) {
            // 执行刷盘操作，将内存中的数据异步写入磁盘。
            flushWriteBuffer(false, false);
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public long maxSequenceNumber() {
        return newSequenceNumber - 1;
    }

    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }
    // 负责将内存中的 writeBuffer（即 LSM 树的 MemTable）刷写到磁盘文件中。这是数据从内存“落地”到持久化存储的关键步骤。
    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        // 首先检查缓冲区中是否有数据。如果缓冲区为空，则无需执行刷盘操作。
        if (writeBuffer.size() > 0) {
            // 是否需要等待最近一次异步合并任务完成
            // 如果后台合并任务堆积过多，为了防止产生过多的 L0 层小文件导致读取性能下降，
            // 这里会强制将 waitForLatestCompaction 设为 true，从而在方法结束前同步等待合并完成（即背压机制）
            if (compactManager.shouldWaitForLatestCompaction()) {
                waitForLatestCompaction = true;
            }
            // 如果 changelog-producer 设置为 input，意味着输入数据本身就是完整的变更流（包含旧值），
            // 此时 Paimon 会启动一个专门的写入器，将内存中的变更数据直接持久化为独立的 changelog 文件。
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    changelogProducer == ChangelogProducer.INPUT
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;
            // 核心的数据文件写入器
            // 负责将内存数据写入 LSM 树的第 0 层 (Level 0)
            // RollingFileWriter 说明它支持“滚动”写入，即当单个文件达到阈值大小时，会自动切换到下一个新文件。
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);

            try {
                // 核心的逻辑。它会遍历内存中的所有数据，并执行以下操作：
                // 排序 (keyComparator)：按照主键（PK）顺序处理数据。
                // 合并 (mergeFunction)：如果在内存中存在相同主键的多次更新，会根据配置的合并引擎（如 deduplicate 或 partial-update）进行合并
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        changelogWriter == null ? null : changelogWriter::write,
                        dataWriter::write);
            } finally {
                writeBuffer.clear();
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }
            // 将新生成的数据文件和 changelog 文件的元数据（如文件路径、大小、统计信息等）分别添加到 newFiles 和 newFilesChangelog 列表中。
            // 这些信息后续将用于快照（Snapshot）的提交。
            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            }

            for (DataFileMeta fileMeta : dataWriter.result()) {
                newFiles.add(fileMeta);
                compactManager.addNewFile(fileMeta);
            }
        }

        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
    }
    // 是 Flink 作业在进行 Checkpoint（检查点）时，由每个写入算子（Writer Operator）调用的核心方法，用于产出本次事务需要提交的元数据。
    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        // 将内存缓冲区（MemTable）中的数据强制刷写到磁盘。
        flushWriteBuffer(waitCompaction, false);
        // 检查是否配置了“提交时强制合并”。
        if (commitForceCompact) {
            waitCompaction = true;
        }
        // Decide again whether to wait here.
        // For example, in the case of repeated failures in writing, it is possible that Level 0
        // files were successfully committed, but failed to restart during the compaction phase,
        // which may result in an increasing number of Level 0 files. This wait can avoid this
        // situation.
        // 根据 compactManager 的状态再次判断是否需要阻塞等待。
        if (compactManager.shouldWaitForPreparingCheckpoint()) {
            waitCompaction = true;
        }
        trySyncLatestCompaction(waitCompaction);
        return drainIncrement();
    }

    @Override
    public boolean compactNotCompleted() {
        compactManager.triggerCompaction(false);
        return compactManager.compactNotCompleted();
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }
    // drainIncrement 是一个至关重要的“收割”方法。
    // 它的名字 drain 形象地描述了它的作用：抽干当前 Writer 中缓存的所有变更信息，并重置状态，为下一次提交做准备。
    private CommitIncrement drainIncrement() {
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        new ArrayList<>(newFilesChangelog));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));
        CompactDeletionFile drainDeletionFile = compactDeletionFile;

        newFiles.clear();
        deletedFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();
        compactDeletionFile = null;

        return new CommitIncrement(dataIncrement, compactIncrement, drainDeletionFile);
    }
    // 负责处理异步合并（Compaction）任务完成后产出的结果，并将其更新到 Writer 的状态中。
    // blocking  如果为 true：当前线程会阻塞等待，直到正在运行的合并任务执行完毕并返回结果。这通常发生在 flush 或 close 时，为了防止后台任务堆积（背压机制）。
    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        // 尝试同步（拉取）最近一次合并任务的结果。
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        // 一旦合并完成，意味着 LSM-Tree 的层级结构发生了变化（例如：几个 Level 0 的文件被合并成了一个更大的 Level 1 文件）
        result.ifPresent(this::updateCompactResult);
    }
    // 当后台合并任务（Compaction）完成后，它负责更新 Writer 内部维护的文件列表，确保哪些文件应该被提交，哪些临时文件可以被物理删除。
    private void updateCompactResult(CompactResult result) {
        // 将合并后产生的新文件（result.after()）的文件名提取到一个 Set 集合中
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        // 遍历合并前的原始文件
        for (DataFileMeta file : result.before()) {
            // 检查这个旧文件是否是在本事务内刚刚产生的“中间文件”
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                // 如果满足条件，说明这是一个完全没用的临时文件，直接调用 writerFactory.deleteFile(file) 从磁盘物理删除，以节省空间
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file);
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
        compactChangelog.addAll(result.changelog());
        // 如果开启了删除向量（Deletion Vectors）模式，合并可能会导致删除标记位的变化，这里需要同步更新对应的 .dv 文件元数据。
        updateCompactDeletionFile(result.deletionFile());
    }

    private void updateCompactDeletionFile(@Nullable CompactDeletionFile newDeletionFile) {
        if (newDeletionFile != null) {
            compactDeletionFile =
                    compactDeletionFile == null
                            ? newDeletionFile
                            : newDeletionFile.mergeOldFile(compactDeletionFile);
        }
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();
        deletedFiles.clear();

        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file);
        }
        newFilesChangelog.clear();

        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }

        compactAfter.clear();

        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file);
        }
        compactChangelog.clear();

        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file);
        }

        if (compactDeletionFile != null) {
            compactDeletionFile.clean();
        }
    }
}

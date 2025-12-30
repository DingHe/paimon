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

import org.apache.paimon.io.DataFileMeta;

import java.util.Collection;
import java.util.List;

/**
 * The {@code RecordWriter} is responsible for writing data and handling in-progress files used to
 * write yet un-staged data. The incremental files ready to commit is returned to the system by the
 * {@link #prepareCommit(boolean)}.
 *
 * @param <T> type of record to write.
 */
// 位于存储引擎的写入端，直接负责将内存中的数据（Record）持久化到文件系统中，并管理与之相关的合并（Compaction）逻辑。
// 主要作用是管理单个分桶（Bucket）内的数据写入生命周期
// 数据持久化网关：它是数据从内存缓冲（如 MemTable）写入到物理磁盘数据文件（Data File）的实际执行者。
// 异步合并调度：它不仅负责写，还负责监控文件的状态。当文件过多时，它会触发异步的 Compaction（合并）任务，以保持 LSM-Tree 结构的健康。
// 事务提交准备：它负责收集当前写入周期内产生的所有新增文件（添加的文件、删除的文件、变更日志等），并封装成 CommitIncrement 提交给上层的 FileStoreCommit。
// 多线程协同：由于 Paimon 采用了异步 Compaction 机制，RecordWriter 负责同步协调写入线程和后台合并线程。
public interface RecordWriter<T> {

    /** Add a key-value element to the writer. */
    // 向写入器添加一条数据记录
    // 对于主键表，数据通常先进入内存中的 MemTable
    // 当 MemTable 达到阈值时，写入器会将其溢写（Flush）到磁盘生成一个新的数据文件。
    void write(T record) throws Exception;

    /**
     * Compact files related to the writer. Note that compaction process is only submitted and may
     * not be completed when the method returns.
     *
     * @param fullCompaction whether to trigger full compaction or just normal compaction
     */
    // 手动触发合并操作。
    // fullCompaction 为 true 时，触发全量合并，将所有层级的文件合并为一层。
    void compact(boolean fullCompaction) throws Exception;

    /**
     * Add files to the internal {@link org.apache.paimon.compact.CompactManager}.
     *
     * @param files files to add
     */
    // 向内部的合并管理器（CompactManager）注入新的文件元数据。
    // 这通常用于外部合并或恢复场景，告知 Writer 某些文件现在属于这个 Bucket，需要纳入 Compaction 的监控范围。
    void addNewFiles(List<DataFileMeta> files);

    /** Get all data files maintained by this writer. */
    // 获取当前写入器所管理的所有数据文件的元数据集合。
    Collection<DataFileMeta> dataFiles();

    /** Get max sequence number of records written by this writer. */
    // 获取该写入器已写入记录的最大序列号（Sequence Number）
    long maxSequenceNumber();

    /**
     * Prepare for a commit.
     *
     * @param waitCompaction if this method need to wait for current compaction to complete
     * @return Incremental files in this snapshot cycle
     */
    // 为准备提交（Commit）做最后的清理和汇总。
    // 参数 waitCompaction：如果为 true，此方法会阻塞，直到当前正在进行的 Compaction 任务完成。
    // 这对于确保 Snapshot 包含完整的合并结果非常重要。
    CommitIncrement prepareCommit(boolean waitCompaction) throws Exception;

    /**
     * Check if a compaction is in progress, or if a compaction result remains to be fetched, or if
     * a compaction should be triggered later.
     */
    // 检查是否存在尚未完成的合并任务。
    boolean compactNotCompleted();

    /**
     * Sync the writer. The structure related to file reading and writing is thread unsafe, there
     * are asynchronous threads inside the writer, which should be synced before reading data.
     */
    // 同步写入器状态。
    // 由于文件读写结构通常是非线程安全的，在执行某些关键读取或元数据操作前，必须调用 sync() 确保所有异步任务已对当前状态可见。
    void sync() throws Exception;

    /** Close this writer, the call will delete newly generated but not committed files. */
    void close() throws Exception;
}

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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/** Manager to submit compaction task. */
// CompactManager 接口扮演着 “合并调度员” 的角色。它是 Paimon 能够维持高性能读写的关键组件，主要负责管理数据文件的合并（Compaction）任务。
// 主要作用是控制和协调数据文件的合并生命周期。
// 由于 Paimon 基于 LSM-Tree（Log-Structured Merge-Tree）架构，频繁的写入会产生大量的小文件。如果任由这些文件堆积，读取性能会急剧下降。CompactManager 的职责就是：
// 策略驱动：决定何时、对哪些文件进行合并。
// 异步执行：管理后台线程中的合并任务，确保合并过程不阻塞主写入流程。
// 结果交付：在合并完成后，将生成的新文件元数据提供给 RecordWriter，以便进行快照（Snapshot）提交。

public interface CompactManager extends Closeable {

    /** Should wait compaction finish. */
    // 判断当前是否需要阻塞等待最近的一个合并任务完成。
    // 在某些场景下（例如为了防止文件积压过多导致的“写放大”），系统需要限制写入速度，通过此方法告知上层：在继续写入前，请先等待后台合并完成。
    boolean shouldWaitForLatestCompaction();
    // 判断在准备 Checkpoint（检查点）时是否需要等待。
    // 为了确保 Checkpoint 的一致性，有时需要确保当前的合并任务已经达到一个可持久化的状态。
    boolean shouldWaitForPreparingCheckpoint();

    /** Add a new file. */
    // 向管理器注册一个新的数据文件。
    // 当 RecordWriter 将内存数据溢写（Flush）到磁盘生成新文件后，会调用此方法。
    // CompactManager 会将该文件加入其监控列表，作为未来合并任务的候选文件。
    void addNewFile(DataFileMeta file);
    // 获取当前管理器维护的所有数据文件元数据。
    // 返回当前 Bucket 内所有活跃的数据文件列表，包括刚写下的新文件和尚未被合并掉的老文件。
    Collection<DataFileMeta> allFiles();

    /**
     * Trigger a new compaction task.
     *
     * @param fullCompaction if caller needs a guaranteed full compaction
     */
    // 手动或自动触发一次合并任务。
    // fullCompaction 为 true：强制触发全量合并（将所有文件合并成最底层的大文件），通常用于离线处理或特定优化。
    void triggerCompaction(boolean fullCompaction);

    /** Get compaction result. Wait finish if {@code blocking} is true. */
    // 获取合并任务的执行结果。
    // blocking 为 true：调用者会阻塞直到合并任务结束并返回结果。
    Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException;

    /** Cancel currently running compaction task. */
    // 取消当前正在运行的合并任务。
    void cancelCompaction();

    /**
     * Check if a compaction is in progress, or if a compaction result remains to be fetched, or if
     * a compaction should be triggered later.
     */
    // 检查合并状态。
    boolean compactNotCompleted();
}

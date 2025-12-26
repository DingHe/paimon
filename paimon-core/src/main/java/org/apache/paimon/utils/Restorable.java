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

import org.apache.paimon.annotation.Public;

/**
 * Operations implementing this interface can checkpoint and restore their states between different
 * instances.
 *
 * @param <S> type of state
 * @since 0.4.0
 */
// Restorable 是一个非常基础且关键的状态管理接口。它主要用于在流式处理或长时间运行的任务中，实现操作状态的保存与恢复。
// 状态持久化支持：允许一个操作实例（如 TableScan 或某种算子）将其内部的中间状态导出（Checkpoint）。
// 容错与连续性：在任务失败重启、或者在分布式作业中进行状态迁移时，通过恢复（Restore）之前的状态，确保任务可以从中断的地方继续运行，而不是从头开始。
// 解耦实例与状态：通过泛型 <S>，它抽象了状态的具体表现形式，使得不同的组件可以根据需要定义自己的状态结构（如快照 ID、文件偏移量等）。

@Public
public interface Restorable<S> {

    /** Extract state of the current operation instance. */
    S checkpoint();

    /** Restore state of a previous operation instance into the current operation instance. */
    void restore(S state);
}

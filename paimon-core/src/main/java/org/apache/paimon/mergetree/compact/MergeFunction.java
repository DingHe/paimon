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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;

/**
 * Merge function to merge multiple {@link KeyValue}s.
 *
 * <p>IMPORTANT, Object reusing inside the kv of the {@link #add} input:
 *
 * <ul>
 *   <li>Please don't save KeyValue and InternalRow references to the List: the KeyValue of the
 *       first two objects and the InternalRow object inside them are safe, but the reference of the
 *       third object may overwrite the reference of the first object.
 *   <li>You can save fields references: fields don't reuse their objects.
 * </ul>
 *
 * @param <T> result type
 */
// 在 Apache Paimon 的存储架构中，MergeFunction 是处理 主键冲突合并逻辑 的核心接口。
// 它定义了如何将具有相同主键（Primary Key）的多条记录，通过特定的算法（如去重、求和、部分更新等）合并为一条最终结果。
// MergeFunction 是 Paimon 实现 “写时优化，读时/合并时聚合” 机制的灵魂。
// 在 LSM 树存储中，数据以 Append-only 的方式写入。当同一个 Key 被多次更新时，物理磁盘上会产生多个版本的 KeyValue。
// MergeFunction 的作用就是在合并（Compaction）或者查询（Read）过程中，将这些版本按照用户定义的逻辑进行压缩。
// 类注释中特别强调了**对象复用（Object Reuse）**的风险。为了极度追求性能，Paimon 的迭代器在扫描数据时会反复使用同一个 KeyValue 和 InternalRow 实例。
// 这意味着在 add 方法中，如果你直接将 kv 的引用存入 List，后续读到的数据可能会覆盖之前的数据。
public interface MergeFunction<T> {

    /**
     * Reset the merge function to its default state, call this before calling {@link
     * #add(KeyValue)} for the first time or after {@link #getResult}.
     */
    // 将合并函数重置为默认状态。
    void reset();

    /** Add the given {@link KeyValue} to the merge function. */
    // 将一条 KeyValue 记录输入到合并算法中。
    void add(KeyValue kv);

    /** Get current merged value. */
    // 计算并返回当前这组 Key 的最终合并结果。
    T getResult();

    /** Require copy input kv, this may cache kv in memory. */
    // 询问合并函数是否需要对输入的 kv 进行深度拷贝。
    boolean requireCopy();
}

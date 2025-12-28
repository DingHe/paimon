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

import javax.annotation.Nullable;

/**
 * A wrapper for {@link MergeFunction}, which adds new functionalities or optimizations.
 *
 * @param <T> result type
 */
// 在 Apache Paimon 的核心存储引擎（Merge Tree）中，MergeFunctionWrapper 是一个至关重要的顶层接口。
// 它定义了如何将具有相同主键（Primary Key）的多条记录进行合并处理的标准流程。
// 在 Paimon 的有主键表中，由于采用了类似 LSM 树的结构，同一个主键的数据可能会多次写入（多次更新或删除）。
// 在读取数据或进行文件压缩（Compaction）时，系统需要将这些碎片化的数据合并为一条最终结果。
public interface MergeFunctionWrapper<T> {
    // 重置当前合并器的内部状态。
    void reset();
    // 向合并器中添加一条待处理的记录。
    // 参数 kv：代表一条 Paimon 内部的 KeyValue 记录，包含主键、序列号、值、记录类型（ADD/DELETE）。
    void add(KeyValue kv);
    // 获取最终的合并结果。
    @Nullable
    T getResult();
}

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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.InternalRow;

import java.io.Serializable;

/**
 * The {@link WriteSelector} determines to which logical downstream writers a record should be
 * written to.
 */
// WriteSelector 的核心作用是 “写操作路由”。
// 在分布式计算框架（如 Flink 或 Spark）中，Sink 端通常会有多个并行度（Writers）。
// 为了保证数据的一致性和提高写入效率，必须决定某一条特定的数据行（InternalRow）应该发送到哪一个并行子任务中去处理。
// 维护数据局部性：确保属于同一个分桶（Bucket）的数据总是被发送到同一个下游 Writer。
// 避免 Shuffle 冲突：在固定分桶（Fixed Bucket）模式下，通过预先计算，使上游算子能够准确地将数据路由到对应的物理节点，减少不必要的网络开销或重复计算。
public interface WriteSelector extends Serializable {

    /** Returns the logical writer index, to which the given record should be written. */
    // InternalRow row：当前待处理的原始数据行。
    // int numWriters：下游物理写入器（Writers）的总并行度数量。
    int select(InternalRow row, int numWriters);
}

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

package org.apache.paimon.table;

import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilderImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ReadBuilderImpl;
import org.apache.paimon.table.source.StreamDataTableScan;

import java.util.Optional;

/** Inner table for implementation, provide newScan, newRead ... directly. */
// InnerTable 扩展了面向用户的 Table 接口。其核心作用可以概括为：暴露表底层的读写组件构造能力。
// 如果说 Table 接口是提供给普通开发者使用的“说明书”，那么 InnerTable 就是提供给 Paimon 内部逻辑和框架开发者（如 Flink/Spark Connector 开发者）的“工具箱”。它打破了 ReadBuilder 或 WriteBuilder 这种高层封装，
// 允许直接创建底层的扫描（Scan）、读取（Read）和写入（Write）组件，从而实现更精细的控制。
// 读取流：InnerTable -> newScan() (找文件) -> newRead() (读内容) -> 返回结果。
// 写入流：InnerTable -> newWrite() (写临时文件) -> 获取提交信息 -> newCommit() (生效 Snapshot)。
public interface InnerTable extends Table {
    // 创建一个内部的批查询扫描器。
    // 用于触发快照（Snapshot）分析，确定需要读取哪些文件（Split）。它不仅能获取当前最新的数据，还支持增量扫描（Incremental Scan）。
    InnerTableScan newScan();
    // 创建一个流式数据扫描器。
    // 专门用于流处理场景（如 flink consume）。它能够持续监听新 Snapshot 的产生，并产生用于流式读取的数据分片。
    StreamDataTableScan newStreamScan();
    // 创建一个内部的读取执行器。
    // 真正执行 IO 操作的类。它会接收 newScan 产生的分片（Split），并利用我们在前几次讨论中提到的 SortMergeReader 或 DataFileRecordReader 来返回实际的数据行。
    InnerTableRead newRead();
    // 创建一个写入选择器。
    // 在分布式写入时（如 Flink 的 Sink 节点），用于决定某条记录应该发往哪一个物理桶（Bucket）。它实现了数据的路由逻辑。
    Optional<WriteSelector> newWriteSelector();
    // 创建一个数据写入器。
    // 接收 commitUser（提交者 ID），返回一个负责在内存中缓冲数据、排序并刷写（Flush）到磁盘文件的组件。
    InnerTableWrite newWrite(String commitUser);
    // 创建一个事务提交器。
    // 负责将 newWrite 产生的临时文件信息封装成 Snapshot，并写入表的 snapshot 目录，完成数据的版本更新。
    InnerTableCommit newCommit(String commitUser);
    // 返回 new ReadBuilderImpl(this)。
    @Override
    default ReadBuilder newReadBuilder() {
        return new ReadBuilderImpl(this);
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        return new BatchWriteBuilderImpl(this);
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        return new StreamWriteBuilderImpl(this);
    }
}

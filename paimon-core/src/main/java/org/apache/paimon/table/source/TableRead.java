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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An abstraction layer above {@link SplitRead} to provide reading of {@link InternalRow}.
 *
 * @since 0.4.0
 */
// 在 Apache Paimon 的读取体系中，TableRead 是负责执行物理读取逻辑的核心接口。
// 如果说 TableScan 是“规划者”（负责找出要读哪些文件），那么 TableRead 就是“执行者”（负责打开文件并转化成数据行）。
// TableRead 处于 ReadBuilder 流程的最后环节。它的主要作用是：
// 物理读取：将 TableScan 产生的逻辑分片（Split）转化为真实的内存对象 InternalRow。
// 底层抽象：它是对底层具体读取操作（如 SplitRead）的封装。无论底层是主键表的合并读取，还是追加表的直接读取，对上层引擎（Flink/Spark）都暴露统一的 TableRead 接口。
// 资源管理：通过它配置 IO 管理器和度量指标，确保读取过程中的临时文件缓存和性能监控得以实施。
@Public
public interface TableRead {

    /** Set {@link MetricRegistry} to table read. */
    // 为读取器注册度量指标。
    // 在流式或批处理作业中，我们需要监控读取的吞吐量、反序列化耗时等。通过传入 MetricRegistry，TableRead 可以将这些数据上报给计算引擎。
    TableRead withMetricRegistry(MetricRegistry registry);
    // 明确开启“在读取时执行过滤器”。
    TableRead executeFilter();
    // 设置 IO 管理器。
    // 在处理主键表（Primary Key Table）时，读取过程可能涉及多路归并排序（External Merge Sort）。
    // 如果内存不足，需要将中间数据溢写到磁盘，IOManager 就负责管理这些临时目录和文件。
    TableRead withIOManager(IOManager ioManager);
    // 核心读取逻辑方法
    // Split（通常包含一组数据文件的路径、偏移量等信息
    // 这是子类必须实现的底层逻辑。它会根据 Split 的类型（如 DataSplit），调用具体的 Parquet/Orc 解码器。
    RecordReader<InternalRow> createReader(Split split) throws IOException;

    // 将多个分片连接起来作为一个整体读取。
    default RecordReader<InternalRow> createReader(List<Split> splits) throws IOException {
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> createReader(split));
        }
        return ConcatRecordReader.create(readers);
    }
    // 直接根据扫描计划创建读取器。
    default RecordReader<InternalRow> createReader(TableScan.Plan plan) throws IOException {
        return createReader(plan.splits());
    }
}

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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Inner {@link TableScan} contains filter push down. */
// InnerTableScan 的核心作用是实现 查询优化中的“谓词下推”（Filter Push Down）和“各种维度的裁剪”。
// 在数据湖查询中，全表扫描是非常昂贵的。InnerTableScan 定义了一系列丰富的配置方法，允许查询引擎（如 Flink/Spark）将各种过滤条件（如分区过滤、主键范围过滤、桶过滤等）直接传递给扫描器。
// 其主要价值体现为：
//减少 I/O 消耗：在读取 Manifest 元数据阶段就过滤掉不符合条件的文件。
//支持复杂查询：不仅支持简单的过滤，还支持 TopN 优化、向量搜索等高级功能。
//流控与优化：支持限制读取行数（Limit）和忽略统计信息（Stats）以加速元数据处理。

public interface InnerTableScan extends TableScan {
    // 下推通用的谓词过滤（如 age > 18）。
    InnerTableScan withFilter(Predicate predicate);
    // 支持向量检索下推，用于 AI/向量数据库场景。
    default InnerTableScan withVectorSearch(VectorSearch vectorSearch) {
        return this;
    }
    // 指定读取的列结构（投影下推）。
    default InnerTableScan withReadType(@Nullable RowType readType) {
        return this;
    }

    default InnerTableScan withLimit(int limit) {
        return this;
    }
    // 根据具体的分区键值对（如 day=20231001）过滤。
    default InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        return this;
    }
    // 同时下推多个分区过滤条件（多分区查询）。
    default InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        return this;
    }

    default InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        return this;
    }

    default InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        return this;
    }

    default InnerTableScan withPartitionFilter(Predicate predicate) {
        return this;
    }

    default InnerTableScan withRowRanges(List<Range> rowRanges) {
        return this;
    }

    default InnerTableScan withBucket(int bucket) {
        return this;
    }

    default InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        return this;
    }

    default InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        return this;
    }

    @Override
    default InnerTableScan withMetricRegistry(MetricRegistry metricRegistry) {
        // do nothing, should implement this if need
        return this;
    }

    default InnerTableScan withTopN(TopN topN) {
        return this;
    }

    default InnerTableScan dropStats() {
        // do nothing, should implement this if need
        return this;
    }
}

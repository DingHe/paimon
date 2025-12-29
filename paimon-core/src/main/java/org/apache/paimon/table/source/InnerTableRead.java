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

import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Inner {@link TableRead} contains filter and projection push down. */
// 扩展了基础的 TableRead 接口，专门负责处理查询优化中的**“下推（Push Down）”**逻辑。
// 在分布式计算中，为了提高性能，通常希望在读取数据的最底层就过滤掉无关的数据（Filter Push Down）或仅读取需要的列（Projection Push Down）。InnerTableRead 充当了“读取执行器”的配置面板：
// 过滤下推：告诉读取器只读取符合 Predicate（谓词）条件的行。
// 投影下推：告诉读取器只返回特定的列，减少 IO 和内存开销。
// 计算下推：支持更高级的 TopN、Limit 等操作在读取阶段完成。
public interface InnerTableRead extends TableRead {

    default InnerTableRead withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    InnerTableRead withFilter(Predicate predicate);

    /** Use {@link #withReadType(RowType)} instead. */
    @Deprecated
    default InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        throw new UnsupportedOperationException();
    }

    default InnerTableRead withReadType(RowType readType) {
        throw new UnsupportedOperationException();
    }

    default InnerTableRead withVariantAccess(VariantAccessInfo[] variantAccessInfo) {
        return this;
    }

    default InnerTableRead withTopN(TopN topN) {
        return this;
    }

    default InnerTableRead withLimit(int limit) {
        return this;
    }

    default InnerTableRead forceKeepDelete() {
        return this;
    }

    @Override
    default TableRead executeFilter() {
        return this;
    }

    @Override
    default InnerTableRead withMetricRegistry(MetricRegistry registry) {
        return this;
    }
}

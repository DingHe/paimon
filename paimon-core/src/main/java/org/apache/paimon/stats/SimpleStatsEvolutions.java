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

package org.apache.paimon.stats;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.schema.SchemaEvolutionUtil.createIndexCastMapping;
import static org.apache.paimon.schema.SchemaEvolutionUtil.devolveFilters;

/** Converters to create col stats array serializer. */
// 主要解决的问题是 Schema Evolution（模式演变） 下的统计信息（Statistics）兼容性问题。
// 在 Paimon 的数据湖架构中，表结构（Schema）是会随着时间发生变化的（如添加列、删除列、更改数据类型）。这意味着：
// 新旧数据并存：磁盘上的旧数据文件可能使用的是 Schema ID 1，而当前最新的表结构是 Schema ID 5。
// 统计信息错位：每个数据文件都带有统计信息（如某列的最大/最小值），这些统计信息的索引位置是基于文件创建时的 Schema。
// 查询转换：当用户用最新的 Schema 发起查询过滤（Predicate）时，需要将其转换成能匹配旧文件结构的过滤条件，以便进行数据跳过（Data Skipping）。
// SimpleStatsEvolutions 的作用就是作为这些演变过程的“翻译官”和“管理器”，确保统计信息和过滤条件在不同版本的 Schema 之间能够正确转换和映射。
public class SimpleStatsEvolutions {
    // 一个回调函数，用于根据 Schema ID 获取该版本对应的所有列字段信息。
    private final Function<Long, List<DataField>> schemaFields;
    // 当前表最新的 Schema ID。
    private final long tableSchemaId;
    // 当前表最新 Schema 的所有字段列表。
    private final List<DataField> tableDataFields;
    // 线程安全地缓存表字段，确保在并发环境下对字段的访问是一致的。
    private final AtomicReference<List<DataField>> tableFields;
    // 缓存器。记录了 旧数据 Schema ID 到 当前表 Schema 的映射关系（SimpleStatsEvolution 对象）。
    private final ConcurrentMap<Long, SimpleStatsEvolution> evolutions;

    public SimpleStatsEvolutions(Function<Long, List<DataField>> schemaFields, long tableSchemaId) {
        this.schemaFields = schemaFields;
        this.tableSchemaId = tableSchemaId;
        this.tableDataFields = schemaFields.apply(tableSchemaId);
        this.tableFields = new AtomicReference<>();
        this.evolutions = new ConcurrentHashMap<>();
    }
    // 根据数据文件的 dataSchemaId 获取对应的演变工具类。
    public SimpleStatsEvolution getOrCreate(long dataSchemaId) {
        return evolutions.computeIfAbsent(
                dataSchemaId,
                id -> {
                    // 如果 ID 相同：说明文件就是按最新结构写的，直接返回一个空的映射关系。
                    if (tableSchemaId == id) {
                        return new SimpleStatsEvolution(
                                new RowType(schemaFields.apply(id)), null, null);
                    }

                    // Get atomic schema fields.
                    // 如果 ID 不同：
                    //
                    //计算索引映射（Index Mapping）：例如旧 Schema 中第 1 列在新 Schema 中变成了第 3 列。
                    //
                    //计算类型转换映射（Cast Mapping）：如果列的数据类型发生了改变（如从 INT 变为 BIGINT），需要记录转换逻辑。
                    //
                    //返回封装好的 SimpleStatsEvolution 对象。
                    List<DataField> schemaTableFields =
                            tableFields.updateAndGet(v -> v == null ? tableDataFields : v);
                    List<DataField> dataFields = schemaFields.apply(id);
                    IndexCastMapping indexCastMapping =
                            createIndexCastMapping(schemaTableFields, schemaFields.apply(id));
                    @Nullable int[] indexMapping = indexCastMapping.getIndexMapping();
                    // Create col stats array serializer with schema evolution
                    return new SimpleStatsEvolution(
                            new RowType(dataFields),
                            indexMapping,
                            indexCastMapping.getCastMapping());
                });
    }

    /**
     * If the file's schema id != current table schema id, convert the filter to evolution safe
     * filter or null if can't.
     */
    // 将“基于最新表结构的过滤器”**退化（Devolve）**为“基于旧数据文件结构的过滤器”。
    @Nullable
    public Predicate tryDevolveFilter(long dataSchemaId, @Nullable Predicate filter) {
        if (filter == null || dataSchemaId == tableSchemaId) {
            return filter;
        }

        // Filter p1 && p2, if only p1 is safe, we can return only p1 to try best filter and let the
        // compute engine to perform p2.
        List<Predicate> filters = PredicateBuilder.splitAnd(filter);
        List<Predicate> devolved =
                devolveFilters(tableDataFields, schemaFields.apply(dataSchemaId), filters, false);

        return devolved.isEmpty() ? null : PredicateBuilder.and(devolved);
    }

    /**
     * Filter unsafe filter, for example, filter is 'a > 9', old type is String, new type is Int, if
     * records are 9, 10 and 11, the evolved filter is not safe.
     */
    // 过滤掉不安全的过滤器。
    // 某些 Schema 变更会导致查询结果不准确。
    // 示例：旧类型是 String，新类型是 Int。在字符串比较中 "10" < "9"，但在数字比较中 10 > 9。
    // 这种类型转换后的比较是不安全的，该方法会将这类可能引起歧义的过滤条件剔除掉，防止因错误的 Data Skipping 导致漏掉数据。
    @Nullable
    public Predicate filterUnsafeFilter(
            long dataSchemaId, @Nullable Predicate filter, boolean keepNewFieldFilter) {
        if (filter == null || dataSchemaId == tableSchemaId) {
            return filter;
        }

        List<Predicate> filters = PredicateBuilder.splitAnd(filter);
        List<DataField> oldSchema = schemaFields.apply(dataSchemaId);
        List<Predicate> result = new ArrayList<>();
        for (Predicate predicate : filters) {
            if (!devolveFilters(
                            tableDataFields,
                            oldSchema,
                            singletonList(predicate),
                            keepNewFieldFilter)
                    .isEmpty()) {
                result.add(predicate);
            }
        }
        return result.isEmpty() ? null : PredicateBuilder.and(result);
    }

    public List<DataField> tableDataFields() {
        return tableDataFields;
    }
}

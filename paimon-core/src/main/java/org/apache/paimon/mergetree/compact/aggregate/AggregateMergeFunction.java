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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastNonNullValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldPrimaryKeyAggFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.ArrayUtils;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record,
 * pre-aggregate non-null fields on merge.
 */
// AggregateMergeFunction 的作用是 “按列执行预定义的聚合逻辑”。
// 与普通的“覆盖（Deduplicate）”或“首行（FirstRow）”引擎不同，聚合引擎允许用户对非主键列分别配置不同的聚合函数（如 sum、max、min、last_non_null_value 等）。
// 当多条具有相同主键的记录到达时，它会将这些记录对应的字段逐一交给相应的聚合器进行处理，最终生成一条汇总后的记录。
public class AggregateMergeFunction implements MergeFunction<KeyValue> {
    // 作用：字段提取器数组。
    // 用于从输入的 InternalRow 中高效地获取特定位置的字段值。
    private final InternalRow.FieldGetter[] getters;
    // 字段聚合器数组。
    // 每个元素对应一列的聚合逻辑（例如第一列是 Sum，第二列是 Max）。
    private final FieldAggregator[] aggregators;
    // 存储各列是否允许为 Null 的标志，用于初始化行时的校验。
    private final boolean[] nullables;
    // 记录最后一次收到的 KeyValue 对象，
    // 主要用于获取其主键、序列号等元数据。
    private KeyValue latestKv;
    // 聚合累加器（中转站）。它存储了当前所有字段聚合后的中间结果。
    private GenericRow row;
    // 为了减少对象创建开销而复用的 KeyValue 对象。
    private KeyValue reused;
    // 标记当前合并过程中是否遇到并处理了删除（DELETE）类型的记录。
    private boolean currentDeleteRow;
    // 决定当收到 DELETE 记录时，是清空整个聚合状态还是执行特定的回撤逻辑。
    private final boolean removeRecordOnDelete;

    public AggregateMergeFunction(
            InternalRow.FieldGetter[] getters,
            FieldAggregator[] aggregators,
            boolean removeRecordOnDelete,
            boolean[] nullables) {
        this.getters = getters;
        this.aggregators = aggregators;
        this.removeRecordOnDelete = removeRecordOnDelete;
        this.nullables = nullables;
    }

    @Override
    public void reset() {
        this.latestKv = null;
        this.row = new GenericRow(getters.length);
        Arrays.stream(aggregators).forEach(FieldAggregator::reset);
        this.currentDeleteRow = false;
    }

    @Override
    public void add(KeyValue kv) {
        latestKv = kv;

        currentDeleteRow = removeRecordOnDelete && kv.valueKind() == RowKind.DELETE;
        if (currentDeleteRow) {
            row = new GenericRow(getters.length);
            initRow(row, kv.value());
            return;
        }

        boolean isRetract = kv.valueKind().isRetract();
        for (int i = 0; i < getters.length; i++) {
            FieldAggregator fieldAggregator = aggregators[i];
            Object accumulator = getters[i].getFieldOrNull(row);
            Object inputField = getters[i].getFieldOrNull(kv.value());
            Object mergedField =
                    isRetract
                            ? fieldAggregator.retract(accumulator, inputField)
                            : fieldAggregator.agg(accumulator, inputField);
            row.setField(i, mergedField);
        }
    }

    private void initRow(GenericRow row, InternalRow value) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(value);
            if (!nullables[i]) {
                if (field != null) {
                    row.setField(i, field);
                } else {
                    throw new IllegalArgumentException("Field " + i + " can not be null");
                }
            }
        }
    }

    @Override
    public KeyValue getResult() {
        checkNotNull(
                latestKv,
                "Trying to get result from merge function without any input. This is unexpected.");

        if (reused == null) {
            reused = new KeyValue();
        }
        RowKind rowKind = currentDeleteRow ? RowKind.DELETE : RowKind.INSERT;
        return reused.replace(latestKv.key(), latestKv.sequenceNumber(), rowKind, row);
    }

    @Override
    public boolean requireCopy() {
        return false;
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Options conf,
            List<String> fieldNames,
            List<DataType> fieldTypes,
            List<String> primaryKeys) {
        return new Factory(conf, fieldNames, fieldTypes, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final CoreOptions options;
        private final List<String> fieldNames;
        private final List<DataType> fieldTypes;
        private final List<String> primaryKeys;
        private final boolean removeRecordOnDelete;

        private Factory(
                Options conf,
                List<String> fieldNames,
                List<DataType> fieldTypes,
                List<String> primaryKeys) {
            this.options = new CoreOptions(conf);
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
            this.primaryKeys = primaryKeys;
            this.removeRecordOnDelete = options.aggregationRemoveRecordOnDelete();
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            List<String> fieldNames = this.fieldNames;
            List<DataType> fieldTypes = this.fieldTypes;
            if (projection != null) {
                Projection project = Projection.of(projection);
                fieldNames = project.project(fieldNames);
                fieldTypes = project.project(fieldTypes);
            }

            FieldAggregator[] fieldAggregators = new FieldAggregator[fieldNames.size()];
            List<String> sequenceFields = options.sequenceField();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                String aggFuncName =
                        getAggFuncName(fieldName, options, primaryKeys, sequenceFields);
                fieldAggregators[i] =
                        FieldAggregatorFactory.create(fieldType, fieldName, aggFuncName, options);
            }

            return new AggregateMergeFunction(
                    createFieldGetters(fieldTypes),
                    fieldAggregators,
                    removeRecordOnDelete,
                    ArrayUtils.toPrimitiveBoolean(
                            fieldTypes.stream().map(DataType::isNullable).toArray(Boolean[]::new)));
        }
    }

    public static String getAggFuncName(
            String fieldName,
            CoreOptions options,
            List<String> primaryKeys,
            List<String> sequenceFields) {

        if (sequenceFields.contains(fieldName)) {
            // no agg for sequence fields, use last_value to do cover
            return FieldLastValueAggFactory.NAME;
        }

        if (primaryKeys.contains(fieldName)) {
            // aggregate by primary keys, so they do not aggregate
            return FieldPrimaryKeyAggFactory.NAME;
        }

        String aggFuncName = options.fieldAggFunc(fieldName);
        if (aggFuncName == null) {
            aggFuncName = options.fieldsDefaultFunc();
        }
        if (aggFuncName == null) {
            // final default agg func
            aggFuncName = FieldLastNonNullValueAggFactory.NAME;
        }
        return aggFuncName;
    }
}

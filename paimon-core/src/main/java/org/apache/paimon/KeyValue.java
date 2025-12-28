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

package org.apache.paimon;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.table.SpecialFields.LEVEL;
import static org.apache.paimon.table.SpecialFields.SEQUENCE_NUMBER;
import static org.apache.paimon.table.SpecialFields.VALUE_KIND;

/**
 * A key value, including user key, sequence number, value kind and value. This object can be
 * reused.
 */
// KeyValue 类是 LSM 树（Log-Structured Merge-tree）存储引擎处理数据的最核心抽象单元。
// KeyValue 类代表了 Paimon 内部的一条“键值对”记录。它不仅包含了用户定义的业务数据，还封装了 Paimon 管理数据版本、合并逻辑以及 LSM 树层级所需的元数据。
// 在 Paimon 的 KeyValue 存储格式中，任何一条写入或读取的数据都会被包装成这个对象。它的设计目标是高性能和可复用性（对象可以被多次替换内容，以减少内存分配和垃圾回收压力）。


public class KeyValue {
    // 常量（-1）。表示序列号尚未确定。
    public static final long UNKNOWN_SEQUENCE = -1;
    // 常量（-1）。表示层级尚未确定。
    public static final int UNKNOWN_LEVEL = -1;
    // 业务主键。对应用户在 DDL 中定义的 PRIMARY KEY 字段。
    private InternalRow key;
    // determined after written into memory table or read from file
    // 序列号。
    // 由 Paimon 自动生成，代表数据的写入顺序。用于在合并时确定哪条数据是“最新的”（Sequence Number 越大，数据越新）。
    private long sequenceNumber;
    // 操作类型。
    // 表示这条记录是新增（+I）、更新（+U/-U）还是删除（-D）。
    private RowKind valueKind;
    // 业务数据值。对应用户表中除去主键以外的其他所有列。
    private InternalRow value;
    // determined after read from file
    // LSM 层级。
    // 记录该记录当前处于 LSM 树的第几层（Level 0 到 Level N）。通常在从文件读取后确定。
    private int level;

    public KeyValue replace(InternalRow key, RowKind valueKind, InternalRow value) {
        return replace(key, UNKNOWN_SEQUENCE, valueKind, value);
    }

    public KeyValue replace(
            InternalRow key, long sequenceNumber, RowKind valueKind, InternalRow value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
        this.level = UNKNOWN_LEVEL;
        return this;
    }

    public KeyValue replaceKey(InternalRow key) {
        this.key = key;
        return this;
    }

    public KeyValue replaceValue(InternalRow value) {
        this.value = value;
        return this;
    }

    public KeyValue replaceValueKind(RowKind valueKind) {
        this.valueKind = valueKind;
        return this;
    }

    public InternalRow key() {
        return key;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public RowKind valueKind() {
        return valueKind;
    }

    public boolean isAdd() {
        return valueKind.isAdd();
    }

    public InternalRow value() {
        return value;
    }

    public int level() {
        return level;
    }

    public KeyValue setLevel(int level) {
        this.level = level;
        return this;
    }

    public static RowType schema(RowType keyType, RowType valueType) {
        return new RowType(false, createKeyValueFields(keyType.getFields(), valueType.getFields()));
    }

    public static RowType schemaWithLevel(RowType keyType, RowType valueType) {
        List<DataField> fields = new ArrayList<>(schema(keyType, valueType).getFields());
        fields.add(LEVEL);
        return new RowType(fields);
    }

    /**
     * Create key-value fields.
     *
     * @param keyFields the key fields
     * @param valueFields the value fields
     * @return the table fields
     */
    public static List<DataField> createKeyValueFields(
            List<DataField> keyFields, List<DataField> valueFields) {
        List<DataField> fields = new ArrayList<>(keyFields.size() + valueFields.size() + 2);
        fields.addAll(keyFields);
        fields.add(SEQUENCE_NUMBER);
        fields.add(VALUE_KIND);
        fields.addAll(valueFields);
        return fields;
    }

    public static int[][] project(
            int[][] keyProjection, int[][] valueProjection, int numKeyFields) {
        int[][] projection = new int[keyProjection.length + 2 + valueProjection.length][];

        // key
        for (int i = 0; i < keyProjection.length; i++) {
            projection[i] = new int[keyProjection[i].length];
            System.arraycopy(keyProjection[i], 0, projection[i], 0, keyProjection[i].length);
        }

        // seq
        projection[keyProjection.length] = new int[] {numKeyFields};

        // value kind
        projection[keyProjection.length + 1] = new int[] {numKeyFields + 1};

        // value
        for (int i = 0; i < valueProjection.length; i++) {
            int idx = keyProjection.length + 2 + i;
            projection[idx] = new int[valueProjection[i].length];
            System.arraycopy(valueProjection[i], 0, projection[idx], 0, valueProjection[i].length);
            projection[idx][0] += numKeyFields + 2;
        }

        return projection;
    }

    @VisibleForTesting
    public KeyValue copy(
            InternalRowSerializer keySerializer, InternalRowSerializer valueSerializer) {
        return new KeyValue()
                .replace(
                        keySerializer.copy(key),
                        sequenceNumber,
                        valueKind,
                        valueSerializer.copy(value))
                .setLevel(level);
    }

    @VisibleForTesting
    public String toString(RowType keyType, RowType valueType) {
        String keyString = rowDataToString(key, keyType);
        String valueString = rowDataToString(value, valueType);
        return String.format(
                "{kind: %s, seq: %d, key: (%s), value: (%s), level: %d}",
                valueKind.name(), sequenceNumber, keyString, valueString, level);
    }

    public static String rowDataToString(InternalRow row, RowType type) {
        return IntStream.range(0, type.getFieldCount())
                .mapToObj(
                        i ->
                                String.valueOf(
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                        type.getTypeAt(i), i)
                                                .getFieldOrNull(row)))
                .collect(Collectors.joining(", "));
    }
}

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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Describes the data type in the paimon ecosystem.
 *
 * @see DataTypes
 * @since 0.4.0
 */
// DataType 是一个至关重要的抽象类。它不仅定义了数据的类型特征，还直接影响了数据的序列化、在磁盘上的存储方式以及与计算引擎（如 Flink/Spark）之间的类型映射。
// 它的主要作用包括：
// 类型定义与标准化：统一描述 Paimon 内部支持的所有数据类型（如 INT, VARCHAR, MAP, ARRAY 等），并与 SQL 标准保持一致。
// Schema 描述：它是构建表结构（Schema）的基本单元。表中的每一列都由一个具体的 DataType 实例来定义。
// 行为契约：定义了一套标准接口，用于类型的复制、空值处理（Nullability）、SQL 字符串化表示以及类型访问者模式（Visitor Pattern）的支持。
// 类型检测与分类：通过 DataTypeRoot 和 DataTypeFamily 机制，方便地判断某个类型是否属于特定的家族（例如：是否是“数值型”家族或“字符串”家族）。
@Public
public abstract class DataType implements Serializable {

    private static final long serialVersionUID = 1L;
    // 标记该数据类型是否允许为 NULL。
    // 在存储优化中，不可为空的列通常可以节省存储空间并提高计算性能。
    private final boolean isNullable;
    // 类型根节点。
    // 这是一个枚举，标识了该类型的本质分类（如 INTEGER、VARCHAR、ARRAY 等），不包含长度或精度等额外参数。
    private final DataTypeRoot typeRoot;

    public DataType(boolean isNullable, DataTypeRoot typeRoot) {
        this.isNullable = isNullable;
        this.typeRoot = Preconditions.checkNotNull(typeRoot);
    }

    /** Returns whether a value of this type can be {@code null}. */
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * Returns the root of this type. It is an essential description without additional parameters.
     */
    public DataTypeRoot getTypeRoot() {
        return typeRoot;
    }

    /**
     * Returns whether the root of the type equals to the {@code typeRoot} or not.
     *
     * @param typeRoot The root type to check against for equality
     */
    // 判断当前类型的根节点是否匹配指定的 typeRoot
    public boolean is(DataTypeRoot typeRoot) {
        return this.typeRoot == typeRoot;
    }

    /**
     * Returns whether the root of the type equals to at least on of the {@code typeRoots} or not.
     *
     * @param typeRoots The root types to check against for equality
     */
    // 判断当前类型是否匹配给定列表中的任意一个根类型。
    public boolean isAnyOf(DataTypeRoot... typeRoots) {
        return Arrays.stream(typeRoots).anyMatch(tr -> this.typeRoot == tr);
    }

    /**
     * Returns whether the root of the type is part of at least one family of the {@code typeFamily}
     * or not.
     *
     * @param typeFamilies The families to check against for equality
     */
    // 判断当前类型是否属于给定列表中的任意一个类型家族。
    public boolean isAnyOf(DataTypeFamily... typeFamilies) {
        return Arrays.stream(typeFamilies).anyMatch(tf -> this.typeRoot.getFamilies().contains(tf));
    }

    /**
     * Returns whether the family type of the type equals to the {@code family} or not.
     *
     * @param family The family type to check against for equality
     */
    public boolean is(DataTypeFamily family) {
        return typeRoot.getFamilies().contains(family);
    }

    /** The default size of a value of this data type, used internally for size estimation. */
    public abstract int defaultSize();

    /**
     * Returns a deep copy of this type with possibly different nullability.
     *
     * @param isNullable the intended nullability of the copied type
     * @return a deep copy
     */
    public abstract DataType copy(boolean isNullable);

    /**
     * Returns a deep copy of this type. It requires an implementation of {@link #copy(boolean)}.
     *
     * @return a deep copy
     */
    public final DataType copy() {
        return copy(isNullable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataType that = (DataType) o;
        return isNullable == that.isNullable && typeRoot == that.typeRoot;
    }

    /**
     * Compare two data types without nullable.
     *
     * @param o the target data type
     */
    public boolean equalsIgnoreNullable(DataType o) {
        return Objects.equals(this.copy(true), o.copy(true));
    }

    /**
     * Compare two data types without field id.
     *
     * @param o the target data type
     */
    public boolean equalsIgnoreFieldId(DataType o) {
        return equals(o);
    }

    /**
     * Determine whether the current type is the result of the target type after pruning (e.g.
     * select some fields from a nested type) or just the same.
     *
     * @param o the target data type
     */
    public boolean isPrunedFrom(Object o) {
        return equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isNullable, typeRoot);
    }

    /**
     * Returns a string that summarizes this type as SQL standard string for printing to a console.
     * An implementation might shorten long names or skips very specific properties.
     */
    public abstract String asSQLString();

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeString(asSQLString());
    }

    protected String withNullability(String format, Object... params) {
        if (!isNullable) {
            return String.format(format + " NOT NULL", params);
        }
        return String.format(format, params);
    }

    @Override
    public String toString() {
        return asSQLString();
    }

    public abstract <R> R accept(DataTypeVisitor<R> visitor);

    public void collectFieldIds(Set<Integer> fieldIds) {}

    public DataType notNull() {
        return copy(false);
    }

    public DataType nullable() {
        return copy(true);
    }
}

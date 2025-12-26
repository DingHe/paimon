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
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static org.apache.paimon.utils.EncodingUtils.escapeIdentifier;
import static org.apache.paimon.utils.EncodingUtils.escapeSingleQuotes;

/**
 * Defines the field of a row type.
 *
 * @since 0.4.0
 */
// DataField 类的主要作用是封装列的定义。它不仅仅包含列名和类型，还承载了 Paimon 湖仓一体能力中至关重要的 Schema 演进（Schema Evolution） 信息
// 身份标识（Identity）：通过唯一的 id 追踪列。在 Paimon 中，即便列被重命名，其 id 保持不变，这使得底层数据文件和元数据能够始终正确匹配。
// 物理与逻辑描述：包含列名（Name）、数据类型（DataType）、注释（Description）以及默认值（Default Value）。
// 版本兼容性支持：提供多种比较方法，用于判断在 Schema 变更前后，两个字段是否逻辑一致或属于包含关系。
@Public
public final class DataField implements Serializable {

    private static final long serialVersionUID = 1L;
    // 核心属性。
    // 字段的唯一标识符（Field ID）。这是 Paimon 处理列增加、删除、重命名的关键，ID 由系统自动分配且不可更改。
    private final int id;
    // 字段名称（逻辑名）
    private final String name;
    // 字段的数据类型（如 IntType, VarCharType 等）
    private final DataType type;
    // 可选的字段描述/注释（COMMENT）
    private final @Nullable String description;
    // 可选的默认值。当读取旧版本数据文件中不存在该列时，可以返回此默认值。
    private final @Nullable String defaultValue;

    public DataField(int id, String name, DataType dataType) {
        this(id, name, dataType, null, null);
    }

    public DataField(int id, String name, DataType dataType, @Nullable String description) {
        this(id, name, dataType, description, null);
    }

    public DataField(
            int id,
            String name,
            DataType type,
            @Nullable String description,
            @Nullable String defaultValue) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }

    public DataType type() {
        return type;
    }

    public DataField newId(int newId) {
        return new DataField(newId, name, type, description, defaultValue);
    }

    public DataField newName(String newName) {
        return new DataField(id, newName, type, description, defaultValue);
    }

    public DataField newType(DataType newType) {
        return new DataField(id, name, newType, description, defaultValue);
    }

    public DataField newDescription(String newDescription) {
        return new DataField(id, name, type, newDescription, defaultValue);
    }

    public DataField newDefaultValue(String newDefaultValue) {
        return new DataField(id, name, type, description, newDefaultValue);
    }

    @Nullable
    public String description() {
        return description;
    }

    @Nullable
    public String defaultValue() {
        return defaultValue;
    }

    public DataField copy() {
        return new DataField(id, name, type.copy(), description, defaultValue);
    }

    public DataField copy(boolean isNullable) {
        return new DataField(id, name, type.copy(isNullable), description, defaultValue);
    }

    public String asSQLString() {
        StringBuilder sb = new StringBuilder();
        sb.append(escapeIdentifier(name)).append(" ").append(type.asSQLString());
        if (StringUtils.isNotEmpty(description)) {
            sb.append(" COMMENT '").append(escapeSingleQuotes(description)).append("'");
        }
        if (defaultValue != null) {
            sb.append(" DEFAULT ").append(defaultValue);
        }
        return sb.toString();
    }

    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField("id", id());
        generator.writeStringField("name", name());
        generator.writeFieldName("type");
        type.serializeJson(generator);
        if (description() != null) {
            generator.writeStringField("description", description());
        }
        if (defaultValue() != null) {
            generator.writeStringField("defaultValue", defaultValue());
        }
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataField field = (DataField) o;
        return Objects.equals(id, field.id)
                && Objects.equals(name, field.name)
                && Objects.equals(type, field.type)
                && Objects.equals(description, field.description)
                && Objects.equals(defaultValue, field.defaultValue);
    }

    public boolean equalsIgnoreFieldId(DataField other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return Objects.equals(name, other.name)
                && type.equalsIgnoreFieldId(other.type)
                && Objects.equals(description, other.description)
                && Objects.equals(defaultValue, other.defaultValue);
    }

    public boolean isPrunedFrom(DataField other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        return Objects.equals(id, other.id)
                && Objects.equals(name, other.name)
                && type.isPrunedFrom(other.type)
                && Objects.equals(description, other.description)
                && Objects.equals(defaultValue, other.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, type, description, defaultValue);
    }

    @Override
    public String toString() {
        return asSQLString();
    }

    /**
     * When the order of the same field is different, its ID may also be different, so the
     * comparison should not include the ID.
     */
    public static boolean dataFieldEqualsIgnoreId(DataField dataField1, DataField dataField2) {
        if (dataField1 == dataField2) {
            return true;
        } else if (dataField1 != null && dataField2 != null) {
            return Objects.equals(dataField1.name(), dataField2.name())
                    && Objects.equals(dataField1.type(), dataField2.type())
                    && Objects.equals(dataField1.description(), dataField2.description())
                    && Objects.equals(dataField1.defaultValue(), dataField2.defaultValue());
        } else {
            return false;
        }
    }
}

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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * The metadata of changelog. It generates from the snapshot file during expiration. So that the
 * changelog of the table can outlive the snapshot's lifecycle. A table's changelog can come from
 * one source:
 * <li>The changelog file. Eg: from the changelog-producer = 'input'
 */
// 在流式数据湖架构中，Snapshot（快照）代表了表在某一时刻的完整状态。然而，为了节省空间，旧的 Snapshot 会被定期清理（Expire）。
// Changelog 类的核心作用在于：解弃变更日志与快照的绑定关系。
// 长生命周期保持：有些下游系统（如 Flink 流式读取）需要消费更久之前的变更数据，即使对应的快照已经因为过期被删除了，Paimon 也可以通过将 Snapshot 转化为 Changelog 元数据文件的方式，保留其变更信息。
// 状态分离：它允许用户设置不同的保留策略。例如：快照只保留 1 小时（用于故障恢复），但变更日志保留 7 天（用于下游增量消费）。
// 统一结构：由于它继承自 Snapshot，它包含了产生该变更时的所有上下文信息（如 Manifest 列表、Schema ID 等）。
@JsonIgnoreProperties(ignoreUnknown = true)
public class Changelog extends Snapshot {

    public Changelog(Snapshot snapshot) {
        this(
                snapshot.version(),
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.baseManifestList(),
                snapshot.baseManifestListSize(),
                snapshot.deltaManifestList(),
                snapshot.deltaManifestListSize(),
                snapshot.changelogManifestList(),
                snapshot.changelogManifestListSize(),
                snapshot.indexManifest(),
                snapshot.commitUser(),
                snapshot.commitIdentifier(),
                snapshot.commitKind(),
                snapshot.timeMillis(),
                snapshot.logOffsets(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.changelogRecordCount(),
                snapshot.watermark(),
                snapshot.statistics(),
                snapshot.properties,
                snapshot.nextRowId);
    }

    @JsonCreator
    public Changelog(
            @JsonProperty(FIELD_VERSION) @Nullable Integer version,
            @JsonProperty(FIELD_ID) long id,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST) String baseManifestList,
            @JsonProperty(FIELD_BASE_MANIFEST_LIST_SIZE) @Nullable Long baseManifestListSize,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST) String deltaManifestList,
            @JsonProperty(FIELD_DELTA_MANIFEST_LIST_SIZE) @Nullable Long deltaManifestListSize,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST) @Nullable String changelogManifestList,
            @JsonProperty(FIELD_CHANGELOG_MANIFEST_LIST_SIZE) @Nullable
                    Long changelogManifestListSize,
            @JsonProperty(FIELD_INDEX_MANIFEST) @Nullable String indexManifest,
            @JsonProperty(FIELD_COMMIT_USER) String commitUser,
            @JsonProperty(FIELD_COMMIT_IDENTIFIER) long commitIdentifier,
            @JsonProperty(FIELD_COMMIT_KIND) CommitKind commitKind,
            @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
            @JsonProperty(FIELD_LOG_OFFSETS) Map<Integer, Long> logOffsets,
            @JsonProperty(FIELD_TOTAL_RECORD_COUNT) @Nullable Long totalRecordCount,
            @JsonProperty(FIELD_DELTA_RECORD_COUNT) @Nullable Long deltaRecordCount,
            @JsonProperty(FIELD_CHANGELOG_RECORD_COUNT) @Nullable Long changelogRecordCount,
            @JsonProperty(FIELD_WATERMARK) @Nullable Long watermark,
            @JsonProperty(FIELD_STATISTICS) @Nullable String statistics,
            @JsonProperty(FIELD_PROPERTIES) Map<String, String> properties,
            @JsonProperty(FIELD_NEXT_ROW_ID) @Nullable Long nextRowId) {
        super(
                version,
                id,
                schemaId,
                baseManifestList,
                baseManifestListSize,
                deltaManifestList,
                deltaManifestListSize,
                changelogManifestList,
                changelogManifestListSize,
                indexManifest,
                commitUser,
                commitIdentifier,
                commitKind,
                timeMillis,
                logOffsets,
                totalRecordCount,
                deltaRecordCount,
                changelogRecordCount,
                watermark,
                statistics,
                properties,
                nextRowId);
    }

    public static Changelog fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Changelog.class);
    }

    public static Changelog fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Fails to read changelog from path " + path, e);
        }
    }

    public static Changelog tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        try {
            String json = fileIO.readFileUtf8(path);
            return Changelog.fromJson(json);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeException("Fails to read changelog from path " + path, e);
        }
    }
}

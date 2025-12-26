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

package org.apache.paimon.consumer;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/** Consumer which contains next snapshot. */
// Consumer 类的作用可以概括为：消费位点的持久化载体。
// 在 Paimon 的流式读取（Streaming Read）过程中，为了保证 Exactly-once（精确一次）或至少一次的消费语义，系统需要将消费进度记录在文件系统中。Consumer 类定义了这些进度文件的内容格式。
// 状态存储：它只存储一个关键信息——下一个待处理的快照 ID (nextSnapshot)。
// 序列化与反序列化：负责将进度信息在内存对象与 JSON 字符串（最终存为文件）之间进行转换。
// 读取鲁棒性：提供带有重试机制的加载方法，确保在分布式环境下读取位点文件时的可靠性。
@JsonIgnoreProperties(ignoreUnknown = true)
public class Consumer {
    // 静态常量，定义 JSON 序列化时使用的字段键名，固定为 "nextSnapshot"。
    private static final String FIELD_NEXT_SNAPSHOT = "nextSnapshot";
    // 核心数据字段。
    // 记录了该消费者期望消费的下一个快照（Snapshot）的序列号（ID）。
    private final long nextSnapshot;

    @JsonCreator
    public Consumer(@JsonProperty(FIELD_NEXT_SNAPSHOT) long nextSnapshot) {
        this.nextSnapshot = nextSnapshot;
    }

    @JsonGetter(FIELD_NEXT_SNAPSHOT)
    public long nextSnapshot() {
        return nextSnapshot;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Consumer fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Consumer.class);
    }

    public static Optional<Consumer> fromPath(FileIO fileIO, Path path) {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 10) {
            Optional<String> content;
            try {
                content = fileIO.readOverwrittenFileUtf8(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            if (!content.isPresent()) {
                return Optional.empty();
            }

            try {
                return content.map(Consumer::fromJson);
            } catch (Exception e) {
                // retry
                exception = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        throw new RuntimeException("Retry fail after 10 times", exception);
    }
}

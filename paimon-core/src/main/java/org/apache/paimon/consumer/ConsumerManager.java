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
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listOriginalVersionedFiles;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;

/** Manage consumer groups. */
// ConsumerManager 是一个负责管理**消费者 ID（Consumer ID）及其消费进度（Offset）**的类。
// 它在流式读取场景下至关重要，类似于 Kafka 的 Consumer Group 偏移量管理。
// ConsumerManager 的主要作用是持久化消费进度。
// 进度跟踪：在流式消费 Paimon 表时，为了保证作业重启后能从上次停止的地方继续，需要记录已消费的 Snapshot ID。
// 防止误删快照：Paimon 会根据这些记录的 nextSnapshot（下一个待消费快照）来辅助判断哪些旧快照是可以安全过期的。如果某个消费者还停留在较早的快照，系统可以据此决定是否保留该快照。
// 多分支支持：管理不同分支（Branch）下的消费者记录。
// 当一个 Flink 任务消费 Paimon 时，流程如下：
// 启动时：调用 consumer(id) 获取上次记录的快照 ID。
// 消费中：定期调用 resetConsumer(id, newPos) 提交进度。
// 快照管理：后台的快照过期任务会调用 minNextSnapshot() 确保不会删掉该任务还需要的数据。
public class ConsumerManager implements Serializable {

    private static final long serialVersionUID = 1L;
    // 静态常量，定义消费者文件的名称前缀，默认为 "consumer-"。
    private static final String CONSUMER_PREFIX = "consumer-";

    private final FileIO fileIO;
    // Paimon 表的根目录路径。
    private final Path tablePath;
    // 当前工作的分支名称，默认是 main 分支。
    private final String branch;

    public ConsumerManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    public ConsumerManager(FileIO fileIO, Path tablePath, String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch =
                StringUtils.isNullOrWhitespaceOnly(branchName) ? DEFAULT_MAIN_BRANCH : branchName;
    }
    // 读取指定 ID 的消费者信息。
    // 它会从 .../consumer/consumer-{id} 路径加载 JSON 文件并解析为 Consumer 对象。
    public Optional<Consumer> consumer(String consumerId) {
        return Consumer.fromPath(fileIO, consumerPath(consumerId));
    }
    // 更新（或创建）消费者的进度。
    // 将最新的快照 ID 写入到对应的消费者文件中。
    public void resetConsumer(String consumerId, Consumer consumer) {
        try {
            fileIO.overwriteFileUtf8(consumerPath(consumerId), consumer.toJson());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    // 物理删除该消费者的进度文件，使系统不再跟踪该消费者的消费位置。
    public void deleteConsumer(String consumerId) {
        fileIO.deleteQuietly(consumerPath(consumerId));
    }
    // 会遍历所有已记录的消费者，找出他们中“最小的” nextSnapshot。
    // 这个值通常作为快照过期（Snapshot Expiration）的重要参考依据，确保不会删除任何消费者仍需读取的快照。
    public OptionalLong minNextSnapshot() {
        try {
            return listOriginalVersionedFiles(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .map(this::consumer)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .mapToLong(Consumer::nextSnapshot)
                    .reduce(Math::min);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    // 根据文件的“最后修改时间”清理不活跃的消费者。如果某个消费者的进度文件在指定时间点之前没有更新过，则将其删除。
    public void expire(LocalDateTime expireDateTime) {
        try {
            listVersionedFileStatus(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .forEach(
                            status -> {
                                LocalDateTime modificationTime =
                                        DateTimeUtils.toLocalDateTime(status.getModificationTime());
                                if (expireDateTime.isAfter(modificationTime)) {
                                    fileIO.deleteQuietly(status.getPath());
                                }
                            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // 正则清理。根据传入的包含正则（including）和排除正则（excluding），批量删除匹配的消费者 ID。这在批量清理测试任务或临时作业的进度时非常有用。
    /** Clear consumers. */
    public void clearConsumers(Pattern includingPattern, Pattern excludingPattern) {
        try {
            listVersionedFileStatus(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .forEach(
                            fileStatus -> {
                                String consumerName =
                                        fileStatus
                                                .getPath()
                                                .getName()
                                                .substring(CONSUMER_PREFIX.length());
                                boolean shouldClear =
                                        includingPattern.matcher(consumerName).matches();
                                if (excludingPattern != null) {
                                    shouldClear =
                                            shouldClear
                                                    && !excludingPattern
                                                            .matcher(consumerName)
                                                            .matches();
                                }
                                if (shouldClear) {
                                    fileIO.deleteQuietly(fileStatus.getPath());
                                }
                            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // Map 形式返回所有消费者的 ID 及其对应的 nextSnapshot ID。
    /** Get all consumer. */
    public Map<String, Long> consumers() throws IOException {
        Map<String, Long> consumers = new HashMap<>();
        listOriginalVersionedFiles(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                .forEach(
                        id -> {
                            Optional<Consumer> consumer = this.consumer(id);
                            consumer.ifPresent(value -> consumers.put(id, value.nextSnapshot()));
                        });
        return consumers;
    }

    /** List all consumer IDs. */
    public List<String> listAllIds() {
        try {
            return listOriginalVersionedFiles(fileIO, consumerDirectory(), CONSUMER_PREFIX)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    // 定位消费者元数据存放的总目录（一般是 table_path/branch/consumer/）。
    private Path consumerDirectory() {
        return new Path(branchPath(tablePath, branch) + "/consumer");
    }
    // 根据消费者 ID 生成具体文件的绝对路径。
    private Path consumerPath(String consumerId) {
        return new Path(
                branchPath(tablePath, branch) + "/consumer/" + CONSUMER_PREFIX + consumerId);
    }
}

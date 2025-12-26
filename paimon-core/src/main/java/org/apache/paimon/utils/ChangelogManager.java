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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.HintFileUtils.commitEarliestHint;
import static org.apache.paimon.utils.HintFileUtils.commitLatestHint;
import static org.apache.paimon.utils.HintFileUtils.findEarliest;
import static org.apache.paimon.utils.HintFileUtils.findLatest;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Manager for {@link Changelog}, providing utility methods related to paths and changelog hints.
 */
// ChangelogManager 是一个专门用于管理 Changelog（变更日志）元数据 的工具类。它的设计模式与 SnapshotManager 非常相似，但它服务的对象是独立存储的变更元文件。
// ChangelogManager 的主要作用是维护和检索 Long-lived Changelog（长周期变更日志）文件。
// 快照（Snapshot）通常包含数据文件的状态，而 Changelog 文件则记录了该快照产生的具体变更
// 独立生命周期：当快照过期被删除时，用户可能希望保留更长时间的变更记录以便进行增量消费。这些被持久化在 changelog/ 目录下的元数据文件由 ChangelogManager 负责。
// 增量查询支持：它允许流式作业根据 Changelog ID 快速定位变更数据，而无需扫描整个 Snapshot 链路。
// 文件定位与读写：提供 Changelog 文件的路径解析、JSON 解析以及读写操作。
public class ChangelogManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogManager.class);
    // 静态常量，定义变更日志文件的名称前缀，默认为 "changelog-"。
    public static final String CHANGELOG_PREFIX = "changelog-";
    // Paimon 的通用文件操作接口，用于执行物理文件的读取、写入和删除。
    private final FileIO fileIO;
    // 当前 Paimon 表的根目录。
    private final Path tablePath;
    // 当前工作的分支名称。Paimon 支持多分支，不同分支的 Changelog 存储在不同子目录下。
    private final String branch;

    public ChangelogManager(FileIO fileIO, Path tablePath, @Nullable String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
    }

    public FileIO fileIO() {
        return fileIO;
    }
    // 获取当前目录下最新的 Changelog ID。它会通过 HintFileUtils 优先读取提示文件，如果提示文件缺失，则通过扫描目录寻找最大的数字后缀。
    public @Nullable Long latestLongLivedChangelogId() {
        try {
            return findLatest(
                    fileIO, changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest changelog id", e);
        }
    }
    // 获取当前目录下最早的可用 Changelog ID。这对于确定增量消费的起始边界非常重要。
    public @Nullable Long earliestLongLivedChangelogId() {
        try {
            return findEarliest(
                    fileIO, changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest changelog id", e);
        }
    }
    // 检查物理文件系统中是否存在指定 ID 的 Changelog 文件。
    public boolean longLivedChangelogExists(long snapshotId) {
        Path path = longLivedChangelogPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if changelog #" + snapshotId + " exists in path " + path,
                    e);
        }
    }
    // 根据 Changelog ID 计算其在文件系统中的完整路径
    public Changelog longLivedChangelog(long snapshotId) {
        return Changelog.fromPath(fileIO, longLivedChangelogPath(snapshotId));
    }
    // 读取指定 ID 的 Changelog 文件，并将其 JSON 内容解析为 Changelog 对象。
    public Changelog changelog(long snapshotId) {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    public Path longLivedChangelogPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }
    // 返回存放变更日志的根目录（changelog/ 目录）
    public Path changelogDirectory() {
        return new Path(branchPath(tablePath, branch) + "/changelog");
    }
    // 将一个 Changelog 对象序列化为 JSON 并写入文件系统。
    public void commitChangelog(Changelog changelog, long id) throws IOException {
        fileIO.writeFile(longLivedChangelogPath(id), changelog.toJson(), true);
    }
    // 更新 _LATEST 提示文件，记录最新的 ID。这能显著降低在高并发读取时频繁 List 目录带来的开销。
    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        commitLatestHint(fileIO, snapshotId, changelogDirectory());
    }
    // 更新 _EARLIEST 提示文件，记录最早的有效 ID。
    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        commitEarliestHint(fileIO, snapshotId, changelogDirectory());
    }
    // 尝试读取 Changelog，如果文件不存在则抛出异常，通常用于需要严格确定元数据完整性的流程。
    public Changelog tryGetChangelog(long snapshotId) throws FileNotFoundException {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.tryFromPath(fileIO, changelogPath);
    }
    // 返回一个迭代器，按 ID 从小到大遍历当前目录下所有的 Changelog 对象
    public Iterator<Changelog> changelogs() throws IOException {
        return listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                .map(this::changelog)
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }
    // 并发安全获取。利用多线程并行读取目录下所有的变更日志文件。在读取过程中如果文件因过期被删除，它会捕获异常并安全跳过，确保作业不会崩溃。
    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                        .map(this::longLivedChangelogPath)
                        .collect(Collectors.toList());

        List<Changelog> changelogs = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        String changelogStr = fileIO.readFileUtf8(path);
                        if (StringUtils.isNullOrWhitespaceOnly(changelogStr)) {
                            LOG.warn("Changelog file is empty, path: {}", path);
                        }
                        changelogs.add(Changelog.fromJson(changelogStr));
                    } catch (IOException e) {
                        if (!(e instanceof FileNotFoundException)) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                paths);

        return changelogs;
    }
    // 动删除对应的提示文件，通常用于元数据清理或重置。
    public void deleteLatestHint() throws IOException {
        HintFileUtils.deleteLatestHint(fileIO, changelogDirectory());
    }

    public void deleteEarliestHint() throws IOException {
        HintFileUtils.deleteEarliestHint(fileIO, changelogDirectory());
    }

    private static void collectSnapshots(Consumer<Path> pathConsumer, List<Path> paths)
            throws IOException {
        ExecutorService executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "CHANGELOG_COLLECTOR");

        try {
            randomlyOnlyExecute(executor, pathConsumer, paths);
        } catch (RuntimeException e) {
            throw new IOException(e);
        } finally {
            executor.shutdown();
        }
    }
}

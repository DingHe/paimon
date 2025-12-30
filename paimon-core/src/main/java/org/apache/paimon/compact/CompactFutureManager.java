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

package org.apache.paimon.compact;

import org.apache.paimon.annotation.VisibleForTesting;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Base implementation of {@link CompactManager} which runs compaction in a separate thread. */
// 专门负责处理异步合并任务的状态管理
// 核心作用是封装了对异步合并任务（Future）的生命周期管理。
public abstract class CompactFutureManager implements CompactManager {
    // 持有了当前正在后台执行的合并任务的引用
    protected Future<CompactResult> taskFuture;
    // 取消当前正在运行的合并任务
    @Override
    public void cancelCompaction() {
        // TODO this method may leave behind orphan files if compaction is actually finished
        //  but some CPU work still needs to be done
        if (taskFuture != null && !taskFuture.isCancelled()) {
            taskFuture.cancel(true);
        }
    }
    // 快速检查合并是否“未完成”
    @Override
    public boolean compactNotCompleted() {
        return taskFuture != null;
    }
    // 内部获取合并结果的核心逻辑
    protected final Optional<CompactResult> innerGetCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        if (taskFuture != null) {
            // 如果参数 blocking 为 true（强制等待），或者任务已经自然结束（isDone()），则进入提取阶段。
            if (blocking || taskFuture.isDone()) {
                CompactResult result;
                try {
                    result = obtainCompactResult();
                } catch (CancellationException e) {
                    return Optional.empty();
                } finally {
                    taskFuture = null;
                }
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    protected CompactResult obtainCompactResult() throws InterruptedException, ExecutionException {
        return taskFuture.get();
    }
}

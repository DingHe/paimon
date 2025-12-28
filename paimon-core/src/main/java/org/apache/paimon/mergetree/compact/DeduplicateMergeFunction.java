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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the full record, only keep
 * the latest one.
 */
// 在 Apache Paimon 的合并引擎（Merge Engine）体系中，DeduplicateMergeFunction 是最常用、也是默认的合并函数。
// DeduplicateMergeFunction 的作用是实现 “去重合并（Deduplicate）” 逻辑。
// 在 Paimon 的主键表中，如果多条记录具有相同的主键，该函数遵循 “最后写入者胜（Last-Write-Wins）” 的原则。它会忽略旧的记录，只保留最新的一条。这类似于传统数据库中的 UPSERT 操作或主键覆盖逻辑。
public class DeduplicateMergeFunction implements MergeFunction<KeyValue> {
    // 决定是否忽略删除类型的记录。
    // 如果设置为 true，那么当收到的记录是 DELETE 或 UPDATE_BEFORE 时，合并函数会直接跳过它，不更新当前状态。
    // 这在某些只需要保留数据、不希望物理删除记录的特殊业务场景中非常有用。
    private final boolean ignoreDelete;
    // 状态存储变量。
    private KeyValue latestKv;

    private DeduplicateMergeFunction(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void reset() {
        latestKv = null;
    }

    @Override
    public void add(KeyValue kv) {
        // In 0.7- versions, the delete records might be written into data file even when
        // ignore-delete configured, so ignoreDelete still needs to be checked
        if (ignoreDelete && kv.valueKind().isRetract()) {
            return;
        }
        latestKv = kv;
    }

    @Override
    public KeyValue getResult() {
        return latestKv;
    }

    @Override
    public boolean requireCopy() {
        return false;
    }

    public static MergeFunctionFactory<KeyValue> factory() {
        return new Factory(false);
    }

    public static MergeFunctionFactory<KeyValue> factory(Options options) {
        return new Factory(options.get(CoreOptions.IGNORE_DELETE));
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final boolean ignoreDelete;

        private Factory(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new DeduplicateMergeFunction(ignoreDelete);
        }
    }
}

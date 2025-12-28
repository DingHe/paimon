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
 * the first one.
 */
// FirstRowMergeFunction 是专门为 “首行保留”策略 设计的核心逻辑实现。
// FirstRowMergeFunction 的作用非常明确：在主键冲突时，永远只保留该主键的第一条记录（最早到达的记录），并忽略后续所有相同主键的更新。
public class FirstRowMergeFunction implements MergeFunction<KeyValue> {
    // 临时存储当前正在处理的主键组中的第一条记录。
    private KeyValue first;
    // 标记当前合并的记录是否包含来自 LSM 树高层级（Level > 0）的数据。
    // 如果为 true，说明该主键在磁盘的旧文件中已经存在。这个标志位非常关键，
    // 它告诉上层的 FirstRowMergeFunctionWrapper 该主键是否是一个“老主键”，从而决定是否要产生 INSERT 类型的 Changelog。
    public boolean containsHighLevel;
    // 配置是否忽略删除类（DELETE / UPDATE_BEFORE）记录。
    // 由于“首行”逻辑通常假设数据是按顺序流入的，默认情况下不支持删除。如果流中出现了删除记录，该属性决定是报错还是静默忽略。
    private final boolean ignoreDelete;

    protected FirstRowMergeFunction(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void reset() {
        this.first = null;
        this.containsHighLevel = false;
    }

    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            // In 0.7- versions, the delete records might be written into data file even when
            // ignore-delete configured, so ignoreDelete still needs to be checked
            if (ignoreDelete) {
                return;
            } else {
                // 若为 false，则抛出异常，因为 FirstRow 引擎默认不支持处理删除。
                throw new IllegalArgumentException(
                        "By default, First row merge engine can not accept DELETE/UPDATE_BEFORE records.\n"
                                + "You can config 'ignore-delete' to ignore the DELETE/UPDATE_BEFORE records.");
            }
        }

        if (first == null) {
            this.first = kv;
        }
        if (kv.level() > 0) {
            containsHighLevel = true;
        }
    }

    @Override
    public KeyValue getResult() {
        return first;
    }

    @Override
    public boolean requireCopy() {
        return true;
    }

    // 从配置项中读取 ignore-delete 参数并创建工厂
    public static MergeFunctionFactory<KeyValue> factory(Options options) {
        return new FirstRowMergeFunction.Factory(options.get(CoreOptions.IGNORE_DELETE));
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;
        private final boolean ignoreDelete;

        public Factory(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new FirstRowMergeFunction(ignoreDelete);
        }
    }
}

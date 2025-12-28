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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.Filter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Wrapper for {@link MergeFunction}s to produce changelog by lookup for first row. */
// 在 Apache Paimon 的合并引擎中，FirstRowMergeFunctionWrapper 是一个专门为 FirstRow 合并引擎（即 'merge-engine' = 'first-row'）设计的包装类。
// 它的核心逻辑是：对于同一个主键，只保留并存储第一条到达的数据，后续重复主键的数据将被丢弃。
// FirstRowMergeFunctionWrapper 的主要作用是实现“首行保留”逻辑并生成相应的变更日志（Changelog）。
// 在数据去重场景中，有时我们只需要记录某个主键第一次出现的时刻（例如用户激活记录）。该类通过与 Lookup 机制配合，判断某个主键是否已经在底层的 L1 层或更高层级（或者通过 Bloom Filter / Index）存在。
// 如果主键已存在：说明这不是“首行”，直接忽略，不输出任何结果。
// 如果主键不存在：说明这是该主键的第一条记录，将其作为新记录输出，并产生一条 INSERT 类型的 Changelog。

public class FirstRowMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {
    // 谓词过滤器，用于判断某个主键是否已经存在于底层的存储中（通常是通过 Lookup 索引实现）
    // 决定“是否跳过当前数据”的关键依据。
    private final Filter<InternalRow> contains;
    // 实际执行“首行”逻辑的合并函数
    // 内部通常逻辑很简单，即在一次合并批次中，如果已经接收到一条数据，就忽略后续数据。
    private final FirstRowMergeFunction mergeFunction;
    // 一个可复用的结果容器。
    private final ChangelogResult reusedResult = new ChangelogResult();

    public FirstRowMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory, Filter<InternalRow> contains) {
        this.contains = contains;
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof FirstRowMergeFunction,
                "Merge function should be a FirstRowMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        this.mergeFunction = (FirstRowMergeFunction) mergeFunction;
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }
    // 该类逻辑最精妙的地方
    @Override
    public ChangelogResult getResult() {
        // 重置 reusedResult
        reusedResult.reset();
        // 从 mergeFunction 拿到当前批次中最先到达的那条 result
        KeyValue result = mergeFunction.getResult();
        // 如果 mergeFunction.containsHighLevel 为 true，说明在 LSM 树的较高层级（L1+）已经存在这个主键了。
        if (mergeFunction.containsHighLevel) {
            // 直接 setResult(result) 并返回。
            // 注意，此时不添加 Changelog，因为这只是数据文件的维护，并不是新产生的增量数据。
            reusedResult.setResult(result);
            return reusedResult;
        }
        // 如果 contains.test(result.key()) 返回 true，说明该主键在之前的 Snapshot 中已经存过了。
        if (contains.test(result.key())) {
            // 直接返回空的 reusedResult（不设结果，不设 Changelog）。这实现了“丢弃非首行数据”的功能。
            // empty
            return reusedResult;
        }

        // new record, output changelog
        // 如果上述判断都通过，说明这是一个完全新出现的主键。
        return reusedResult.setResult(result).addChangelog(result);
    }
}

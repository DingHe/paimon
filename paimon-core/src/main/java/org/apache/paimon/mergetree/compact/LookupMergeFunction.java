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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Comparator;

/**
 * A {@link MergeFunction} for lookup, this wrapper only considers the latest high level record,
 * because each merge will query the old merged record, so the latest high level record should be
 * the final merged value.
 */
// LookupMergeFunction 的核心作用是：优化 LSM 树中跨层级的数据合并，特别是配合 Lookup 索引来查找并合并旧数据。
// 在 Paimon 的 LSM 树中，Level 0 的文件是无序且可能重叠的，而 Level 1 及更高层级的文件是全局有序的。
// 该类的设计逻辑基于一个核心假设：高层级（Level > 0）中最新的记录已经代表了该记录在磁盘上的最终合并状态。
// 通过 Lookup 机制，Paimon 可以快速定位到某个 Key 在高层级中对应的唯一记录，然后将其取出，只与内存中（Level -1）或 Level 0 中的新记录进行合并。
// 这样可以避免扫描所有层级的文件，极大提升合并效率。
public class LookupMergeFunction implements MergeFunction<KeyValue> {
    // 被包装的实际合并逻辑（如 Deduplicate 或 Aggregate）。
    // 此装饰器只负责筛选数据，最终合并仍由它完成。
    private final MergeFunction<KeyValue> mergeFunction;
    // 候选数据缓冲区。
    // 用于暂存当前处理的主键（Key）对应的所有不同版本的 KeyValue。它是一个混合缓冲区（Hybrid Buffer），当数据量大时可以溢写到磁盘。
    private final KeyValueBuffer candidates;
    // 标记当前处理的主键是否包含来自 Level 0 或内存（Level <= 0）的记录。
    private boolean containLevel0;
    // 当前正在处理的业务主键。
    private InternalRow currentKey;

    public LookupMergeFunction(
            MergeFunction<KeyValue> mergeFunction,
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.mergeFunction = mergeFunction;
        this.candidates = KeyValueBuffer.createHybridBuffer(options, keyType, valueType, ioManager);
    }

    @Override
    public void reset() {
        candidates.reset();
        currentKey = null;
        containLevel0 = false;
    }
    // 将收到的 kv 放入 candidates 缓冲区。同时检查该 kv 的层级，如果是 Level 0，则更新 containLevel0 标记。
    @Override
    public void add(KeyValue kv) {
        currentKey = kv.key();
        if (kv.level() == 0) {
            containLevel0 = true;
        }
        candidates.put(kv);
    }

    public boolean containLevel0() {
        return containLevel0;
    }
    // 从候选记录中寻找 Level 最小但大于 0 的记录。
    // 原理：在 LSM 中，Level 越小数据越新。由于高层级（L1+）每层内部有序，所以 L1 的数据一定比 L2 新。选出 Level 最小的高层记录就代表了磁盘上的最新状态。
    @Nullable
    public KeyValue pickHighLevel() {
        KeyValue highLevel = null;
        try (CloseableIterator<KeyValue> iterator = candidates.iterator()) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                // records that has not been stored on the disk yet, such as the data in the write
                // buffer being at level -1
                if (kv.level() <= 0) {
                    continue;
                }
                // For high-level comparison logic (not involving Level 0), only the value of the
                // minimum Level should be selected
                if (highLevel == null || kv.level() < highLevel.level()) {
                    highLevel = kv;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return highLevel;
    }

    public InternalRow key() {
        return currentKey;
    }
    // 手动将外部 Lookup 查到的高层级数据注入缓冲区。
    public void insertInto(KeyValue highLevel, Comparator<KeyValue> comparator) {
        KeyValueBuffer.insertInto(candidates, highLevel, comparator);
    }
    // 先调用 pickHighLevel() 找到高层级中最新的那条记录。
    // 遍历缓冲区中所有记录。
    // 筛选规则：只将“新数据”（Level <= 0）和“选出的最高层级数据”喂给底层的 mergeFunction。
    // 返回底层合并后的最终结果。
    @Override
    public KeyValue getResult() {
        mergeFunction.reset();
        KeyValue highLevel = pickHighLevel();
        try (CloseableIterator<KeyValue> iterator = candidates.iterator()) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                // records that has not been stored on the disk yet, such as the data in the write
                // buffer being at level -1
                if (kv.level() <= 0 || kv == highLevel) {
                    mergeFunction.add(kv);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mergeFunction.getResult();
    }

    @Override
    public boolean requireCopy() {
        return true;
    }

    public static MergeFunctionFactory<KeyValue> wrap(
            MergeFunctionFactory<KeyValue> wrapped,
            CoreOptions options,
            RowType keyType,
            RowType valueType) {
        if (wrapped.create() instanceof FirstRowMergeFunction) {
            // don't wrap first row, it is already OK
            return wrapped;
        }

        return new Factory(wrapped, options, keyType, valueType);
    }

    /** Factory to create {@link LookupMergeFunction}. */
    public static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped;
        private final CoreOptions options;
        private final RowType keyType;
        private final RowType valueType;

        private @Nullable IOManager ioManager;

        private Factory(
                MergeFunctionFactory<KeyValue> wrapped,
                CoreOptions options,
                RowType keyType,
                RowType valueType) {
            this.wrapped = wrapped;
            this.options = options;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public void withIOManager(@Nullable IOManager ioManager) {
            this.ioManager = ioManager;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new LookupMergeFunction(
                    wrapped.create(projection), options, keyType, valueType, ioManager);
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            return wrapped.adjustProjection(projection);
        }
    }
}

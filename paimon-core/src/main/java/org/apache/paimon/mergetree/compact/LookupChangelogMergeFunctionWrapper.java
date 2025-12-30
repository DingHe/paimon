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
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.lookup.FilePosition;
import org.apache.paimon.mergetree.lookup.PositionedKeyValue;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Wrapper for {@link MergeFunction}s to produce changelog by lookup during the compaction involving
 * level 0 files.
 *
 * <p>Changelog records are generated in the process of the level-0 file participating in the
 * compaction, if during the compaction processing:
 *
 * <ul>
 *   <li>Without level-0 records, no changelog.
 *   <li>With level-0 record, with level-x (x > 0) record, level-x record should be BEFORE, level-0
 *       should be AFTER.
 *   <li>With level-0 record, without level-x record, need to lookup the history value of the upper
 *       level as BEFORE.
 * </ul>
 */
// 在包含 Level 0 文件的 Compaction（压缩合并）过程中，通过反向查找（Lookup）历史数据来生成精确的变更日志（Changelog）。
// 在 LSM-Tree 中，Level 0 的文件是乱序且可能包含重复 Key 的。当 L0 文件参与合并时，为了让下游知道数据到底发生了什么变化（是新增、更新还是删除），这个类会：
// 收集当前参与合并的所有 KeyValue。
// 如果没有高层级（Level > 0）的旧数据，它会调用 lookup 函数去更深层的文件中查找该 Key 的“前世（Before Value）”。
// 对比“前世”与“今生（Merged Result）”，计算出 INSERT、UPDATE_BEFORE、UPDATE_AFTER 或 DELETE 等消息。

public class LookupChangelogMergeFunctionWrapper<T>
        implements MergeFunctionWrapper<ChangelogResult> {
    // 实际执行数据聚合逻辑的对象（如求和、保留最新等）。
    // 它必须是 LookupMergeFunction 类型，因为它提供了识别数据层级的能力。
    private final LookupMergeFunction mergeFunction;
    // 当本地合并的文件中没有旧数据时，
    // 调用此函数从更高层级的索引（SstFile/LookupFile）中查询数据。
    private final Function<InternalRow, T> lookup;
    // 为了减少垃圾回收（GC）压力而设计的重用对象，分别用于承载最终结果、变更前快照和变更后快照。
    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();
    // 用于判断两个 Value 是否在逻辑上完全相等
    @Nullable private final RecordEqualiser valueEqualiser;
    // 决定是否需要产生 Changelog 以及是否启用 Deletion Vector（删除向量）。
    private final LookupStrategy lookupStrategy;
    private final @Nullable BucketedDvMaintainer deletionVectorsMaintainer;
    private final Comparator<KeyValue> comparator;

    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            @Nullable RecordEqualiser valueEqualiser,
            LookupStrategy lookupStrategy,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof LookupMergeFunction,
                "Merge function should be a LookupMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        if (lookupStrategy.deletionVector) {
            checkArgument(
                    deletionVectorsMaintainer != null,
                    "deletionVectorsMaintainer should not be null, there is a bug.");
        }
        this.mergeFunction = (LookupMergeFunction) mergeFunction;
        this.lookup = lookup;
        this.valueEqualiser = valueEqualiser;
        this.lookupStrategy = lookupStrategy;
        this.deletionVectorsMaintainer = deletionVectorsMaintainer;
        this.comparator = createSequenceComparator(userDefinedSeqComparator);
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    @Override
    public ChangelogResult getResult() {
        // 1. Find the latest high level record and compute containLevel0
        KeyValue highLevel = mergeFunction.pickHighLevel();
        boolean containLevel0 = mergeFunction.containLevel0();

        // 2. Lookup if latest high level record is absent
        if (highLevel == null) {
            T lookupResult = lookup.apply(mergeFunction.key());
            if (lookupResult != null) {
                if (lookupStrategy.deletionVector) {
                    String fileName;
                    long rowPosition;
                    if (lookupResult instanceof PositionedKeyValue) {
                        PositionedKeyValue positionedKeyValue = (PositionedKeyValue) lookupResult;
                        highLevel = positionedKeyValue.keyValue();
                        fileName = positionedKeyValue.fileName();
                        rowPosition = positionedKeyValue.rowPosition();
                    } else {
                        FilePosition position = (FilePosition) lookupResult;
                        fileName = position.fileName();
                        rowPosition = position.rowPosition();
                    }
                    deletionVectorsMaintainer.notifyNewDeletion(fileName, rowPosition);
                } else {
                    highLevel = (KeyValue) lookupResult;
                }
            }
            if (highLevel != null) {
                mergeFunction.insertInto(highLevel, comparator);
            }
        }

        // 3. Calculate result
        KeyValue result = mergeFunction.getResult();

        // 4. Set changelog when there's level-0 records
        reusedResult.reset();
        if (containLevel0 && lookupStrategy.produceChangelog) {
            setChangelog(highLevel, result);
        }

        return reusedResult.setResult(result);
    }

    private void setChangelog(@Nullable KeyValue before, KeyValue after) {
        if (before == null || !before.isAdd()) {
            if (after.isAdd()) {
                reusedResult.addChangelog(replaceAfter(RowKind.INSERT, after));
            }
        } else {
            if (!after.isAdd()) {
                reusedResult.addChangelog(replaceBefore(RowKind.DELETE, before));
            } else if (valueEqualiser == null
                    || !valueEqualiser.equals(before.value(), after.value())) {
                reusedResult
                        .addChangelog(replaceBefore(RowKind.UPDATE_BEFORE, before))
                        .addChangelog(replaceAfter(RowKind.UPDATE_AFTER, after));
            }
        }
    }

    private KeyValue replaceBefore(RowKind valueKind, KeyValue from) {
        return replace(reusedBefore, valueKind, from);
    }

    private KeyValue replaceAfter(RowKind valueKind, KeyValue from) {
        return replace(reusedAfter, valueKind, from);
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }

    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            return Comparator.comparingLong(KeyValue::sequenceNumber);
        }

        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o1.value(), o2.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o1.sequenceNumber(), o2.sequenceNumber());
        };
    }
}

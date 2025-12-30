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
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/** Default {@link CompactRewriter} for merge trees. */
// MergeTreeCompactRewriter 是 CompactRewriter 接口的默认物理实现类。
// 如果说合并策略（Strategy）决定了“合并哪些文件”，那么这个类就负责**“如何把这些文件里的数据读出来、归并排序、并写成新文件”**。
// 核心作用是执行 LSM-Tree 的归并重写。 它通过读取多个 SortedRun 中的原始数据，利用 MergeSorter 进行多路归并排序，并调用用户定义的 MergeFunction（如去重、求和、保留最新等）处理相同主键的数据，最后将结果持久化到指定层级的新数据文件中。
// 它是实现 Paimon 数据一致性和存储紧凑性的核心引擎。
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {
    // 数据读取工厂。
    // 用于创建读取 KeyValue 数据的 RecordReader，负责从磁盘加载原始合并文件。
    protected final FileReaderFactory<KeyValue> readerFactory;
    // 数据写入工厂。
    // 用于创建 KeyValueFileWriter，负责将合并后的结果写回磁盘。
    protected final KeyValueFileWriterFactory writerFactory;
    // 主键比较器。在归并排序过程中，用于确定 Key 的先后顺序。
    protected final Comparator<InternalRow> keyComparator;
    // 用户自定义序列号比较器（可选）。
    // 当 Key 相同时，通过它来决定哪个版本的数据更新（例如基于 sequence_number 或用户指定的字段）
    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    // 合并函数工厂。
    // 用于创建 MergeFunction，定义了当多个 KeyValue 具有相同 Key 时，应该如何“聚合”或“覆盖”它们。
    protected final MergeFunctionFactory<KeyValue> mfFactory;
    // 归并排序器。
    // 执行底层的大规模数据排序算法，将多个有序流（Sorted Runs）合并为一个单一有序流。
    protected final MergeSorter mergeSorter;

    public MergeTreeCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.userDefinedSeqComparator = userDefinedSeqComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter = mergeSorter;
    }
    // 重写操作的入口。
    // 直接调用内部的 rewriteCompaction 方法。这种设计是为了方便子类在 rewrite 基础上增加额外逻辑（如 Changelog 生成）
    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(outputLevel, dropDelete, sections);
    }
    // 最核心的逻辑
    protected CompactResult rewriteCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        // 创建写入器：
        // 通过 writerFactory 创建一个 RollingFileWriter。它可以根据文件大小自动切分（Rolling）新生成的数据文件。
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
        RecordReader<KeyValue> reader = null;
        Exception collectedExceptions = null;
        try {
            // 构建读取器流：
            // 调用 readerForMergeTree 将分散的 sections 转化为一个统一的、经过归并排序后的 RecordReader。
            reader =
                    readerForMergeTree(
                            sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
            // 丢弃删除标记：如果 dropDelete 为真，则在外层包装一个 DropDeleteReader。
            // 这通常发生在合并到最大层时，物理删除所有的 DELETE 类型记录。
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }
            // 将 RecordReader 的迭代器传入 writer.write(...)。此时，数据从旧文件流向新文件，并在流动过程中完成合并。
            writer.write(new RecordReaderIterator<>(reader));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }
        // 提取 before（合并前的文件）和 after（生成的新文件），返回 CompactResult。
        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before);
        List<DataFileMeta> after = writer.result();
        after = notifyRewriteCompactAfter(after);
        return new CompactResult(before, after);
    }
    // 构建合并专用的读取器链。
    // 将多个 SortedRun 包装成一个具有多路归并能力的 RecordReader
    protected <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections, MergeFunctionWrapper<T> mergeFunctionWrapper)
            throws IOException {
        return MergeTreeReaders.readerForMergeTree(
                sections,
                readerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mergeFunctionWrapper,
                mergeSorter);
    }

    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}

    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        return files;
    }
}

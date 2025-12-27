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

package org.apache.paimon.mergetree;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link SortedRun} is a list of files sorted by their keys. The key intervals [minKey, maxKey]
 * of these files do not overlap.
 */
// 在 Apache Paimon 的 Merge Tree（合并树）架构中，SortedRun 是一个非常基础且核心的概念。它代表了一组在物理和逻辑上都“严格有序且不重叠”的数据文件集合。
// SortedRun 的直译是“有序运行链”。它的核心作用是：封装一组互不重叠的文件，使其在逻辑上可以被视为一个单一的、连续的有序数据流。
// 在 LSM Tree 中，数据被分成多个文件存储。如果直接对成百上千个文件进行归并排序，开销会非常大。
// 逻辑合并：SortedRun 将多个物理文件组合在一起。只要这些文件之间没有主键（Key）重叠，读取时就可以像读取一个超大有序文件一样，顺序读取完文件 A 后紧接着读取文件 B。
// 减少归并开销：在 IntervalPartition 算法中，目标就是将文件划分为最少数量的 SortedRun。因为归并排序的复杂度与 SortedRun 的数量成正比，SortedRun 越少，读取性能越好。


public class SortedRun {
    // 核心数据。
    // 一个不可变的列表，按主键顺序存储了该运行链包含的所有数据文件的元数据。
    private final List<DataFileMeta> files;
    // 缓存大小。
    // 该运行链中所有文件的字节数之和。用于在分片（Split）计算或压缩（Compact）决策时快速获取数据量。
    private final long totalSize;

    private SortedRun(List<DataFileMeta> files) {
        this.files = Collections.unmodifiableList(files);
        long totalSize = 0L;
        for (DataFileMeta file : files) {
            totalSize += file.fileSize();
        }
        this.totalSize = totalSize;
    }

    public static SortedRun empty() {
        return new SortedRun(Collections.emptyList());
    }

    public static SortedRun fromSingle(DataFileMeta file) {
        return new SortedRun(Collections.singletonList(file));
    }

    public static SortedRun fromSorted(List<DataFileMeta> sortedFiles) {
        return new SortedRun(sortedFiles);
    }

    public static SortedRun fromUnsorted(
            List<DataFileMeta> unsortedFiles, Comparator<InternalRow> keyComparator) {
        unsortedFiles.sort((o1, o2) -> keyComparator.compare(o1.minKey(), o2.minKey()));
        SortedRun run = new SortedRun(unsortedFiles);
        run.validate(keyComparator);
        return run;
    }

    public List<DataFileMeta> files() {
        return files;
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    public boolean nonEmpty() {
        return !isEmpty();
    }

    public long totalSize() {
        return totalSize;
    }

    // 正确性保证的关键。
    // 它会遍历文件，验证前一个文件的 maxKey 必须小于后一个文件的 minKey。如果发现重叠或乱序，会抛出异常。这在开发和调试阶段对于防止数据损坏非常重要。
    @VisibleForTesting
    public void validate(Comparator<InternalRow> comparator) {
        for (int i = 1; i < files.size(); i++) {
            Preconditions.checkState(
                    comparator.compare(files.get(i).minKey(), files.get(i - 1).maxKey()) > 0,
                    "SortedRun is not sorted and may contain overlapping key intervals. This is a bug.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortedRun)) {
            return false;
        }
        SortedRun that = (SortedRun) o;
        return files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    @Override
    public String toString() {
        return "["
                + files.stream().map(DataFileMeta::toString).collect(Collectors.joining(", "))
                + "]";
    }
}

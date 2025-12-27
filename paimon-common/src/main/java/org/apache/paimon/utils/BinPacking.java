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

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

import static java.util.Comparator.comparingLong;

/** Contains bin packing implementations. */
// 实现了经典的装箱算法（Bin Packing Problem）。
// 它的核心目的是将一组具有不同“权重”（通常指文件大小或记录数）的元素，合理地分配到若干个“桶（Bin）”中，以达到负载均衡或控制单个任务处理量的目的。
// 负载均衡：在读取数据时，将许多小文件合并为一个 Split，确保每个读取任务处理的数据量相对均匀，避免出现“长尾任务”。
// 控制资源消耗：通过 targetWeight（目标权重）控制每个分片的大小，防止单个算子内存溢出。
// 两种模式：
// 保持顺序（Ordered）：在处理 LSM 树的 Section 或 SortedRun 时，必须保持主键的先后顺序，不能打乱元素位置。
// 固定桶数（Fixed Bin Number）：在已知并行度的情况下，将元素尽可能均匀地散列到固定数量的桶中（类似于计算引擎的调度优化）。
public class BinPacking {
    private BinPacking() {}

    /** Ordered packing for input items. */
    // 有序装箱
    // items: 待装箱的元素列表（如 DataFileMeta 或 Section）
    // weightFunc: 定义权重的函数（通常返回文件大小）。
    // targetWeight: 每个桶期望达到的最大权重（如配置的 split.target-size）
    public static <T> List<List<T>> packForOrdered(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();

        List<T> binItems = new ArrayList<>();
        long binWeight = 0L;
        // 从头到尾扫描 items，不改变元素的原始顺序
        for (T item : items) {
            long weight = weightFunc.apply(item);
            // when get a much big item or total weight enough, we check the binItems size. If
            // greater than zero, we pack it
            // 累加判断：将元素放入当前桶，如果加入新元素后总权重超过了 targetWeight，且当前桶不是空的，则将当前桶“封口”存入结果集。
            if (binWeight + weight > targetWeight && binItems.size() > 0) {
                packed.add(binItems);
                binItems = new ArrayList<>();
                binWeight = 0;
            }

            binWeight += weight;
            binItems.add(item);
        }

        if (binItems.size() > 0) {
            packed.add(binItems);
        }
        return packed;
    }

    /** A bin packing implementation for fixed bin number. */
    // 固定桶数装箱
    // binNumber: 指定最终要生成的桶的数量（即并行度）
    public static <T> List<List<T>> packForFixedBinNumber(
            Iterable<T> items, Function<T, Long> weightFunc, int binNumber) {
        // 1. sort items first
        // 先将所有元素按权重从小到大排序
        List<T> sorted = new ArrayList<>();
        items.forEach(sorted::add);
        sorted.sort(comparingLong(weightFunc::apply));

        // 2. packing
        // 使用一个**优先队列（最小堆）**维护所有的桶。
        PriorityQueue<FixedNumberBin<T>> bins = new PriorityQueue<>();
        for (T item : sorted) {
            // 每次取出一个当前“最轻”的桶（总权重最小），把当前处理的元素放进去，再放回队列。
            long weight = weightFunc.apply(item);
            FixedNumberBin<T> bin = bins.size() < binNumber ? new FixedNumberBin<>() : bins.poll();
            bin.add(item, weight);
            bins.add(bin);
        }

        // 3. output
        List<List<T>> packed = new ArrayList<>();
        bins.forEach(bin -> packed.add(bin.items));
        return packed;
    }

    private static class FixedNumberBin<T> implements Comparable<FixedNumberBin<T>> {
        private final List<T> items = new ArrayList<>();
        private long binWeight = 0L;

        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }

        @Override
        public int compareTo(FixedNumberBin<T> other) {
            return Long.compare(binWeight, other.binWeight);
        }
    }
}

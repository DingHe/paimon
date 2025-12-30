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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link LookupStoreWriter} backed by an {@link SstFileWriter}. The SST File layout is as below:
 * (For layouts of each block type, please refer to corresponding classes)
 *
 * <pre>
 *     +-----------------------------------+------+
 *     |             Footer                |      |
 *     +-----------------------------------+      |
 *     |           Index Block             |      +--> Loaded on open
 *     +-----------------------------------+      |
 *     |        Bloom Filter Block         |      |
 *     +-----------------------------------+------+
 *     |            Data Block             |      |
 *     +-----------------------------------+      |
 *     |              ......               |      +--> Loaded on requested
 *     +-----------------------------------+      |
 *     |            Data Block             |      |
 *     +-----------------------------------+------+
 * </pre>
 */
// SortLookupStoreWriter 是 Apache Paimon 中基于 SST (Sorted String Table) 布局的本地索引写入器实现。
// 它是 lookup 索引持久化的物理层核心类，直接决定了索引文件在磁盘上的组织结构。
// 该类的主要作用是构建一个兼容 SST 格式的二进制文件。
// 它封装了 SstFileWriter，将用户传入的键值对（Key-Value）组织成多个块（Blocks），并生成辅助查询的元数据（索引块、布隆过滤器、脚注）。
// 其文件布局结构（如代码注释所示）：
//Data Blocks（数据块）：按 Key 有序存储实际的二进制数据，支持压缩。
//Bloom Filter Block（布隆过滤器块）：用于在读取时快速判断某个 Key 是否肯定不存在。
//Index Block（索引块）：存储每个 Data Block 的起始位置和该块中最大的 Key，用于二分查找。
//Footer（脚注）：存储索引块和布隆过滤器的位置偏移量，是读取文件的“入口”。


public class SortLookupStoreWriter implements LookupStoreWriter {
    // 底层真正的 SST 构建器。
    // 它负责处理具体的块切分（当数据达到 blockSize 时溢写）、Key 的前缀压缩、布隆过滤器的生成以及数据块的落盘。
    private final SstFileWriter writer;
    // 文件输出流。
    // 它不仅负责向磁盘写数据，还记录了当前的物理偏移量（Position），这对于生成索引块中各数据块的地址（Handles）至关重要。
    private final PositionOutputStream out;

    public SortLookupStoreWriter(
            PositionOutputStream out, // 文件流。
            int blockSize, // 每个 Data Block 的目标大小（通常为 32KB 或 64KB）
            @Nullable BloomFilter.Builder bloomFilter, // 布隆过滤器构建器，用于记录已写入的所有 Key
            BlockCompressionFactory compressionFactory) {
        this.out = out;
        this.writer = new SstFileWriter(out, blockSize, bloomFilter, compressionFactory);
    }
    // 写入一条数据。
    // Key 必须按照升序顺序写入。只有有序写入，后续才能生成有效的索引块并支持点查时的二分检索。
    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        writer.put(key, value);
    }
    // 构建索引文件的最关键阶段，它按照倒序完成文件的收尾工作
    @Override
    public void close() throws IOException {
        // 将当前内存中最后剩余的数据打包成最后一个 Data Block 写入磁盘
        writer.flush();
        // 根据已处理的所有 Key，计算并写出布隆过滤器块，返回其位置 bloomFilterHandle。
        BloomFilterHandle bloomFilterHandle = writer.writeBloomFilter();
        // 写出索引块（记录了所有 Data Block 的范围），返回其位置 indexBlockHandle。
        BlockHandle indexBlockHandle = writer.writeIndexBlock();
        // 将布隆过滤器和索引块的位置封装进 SortLookupStoreFooter 对象
        SortLookupStoreFooter footer =
                new SortLookupStoreFooter(bloomFilterHandle, indexBlockHandle);
        MemorySlice footerEncoding = SortLookupStoreFooter.writeFooter(footer);
        writer.writeSlice(footerEncoding);
        out.close();
    }
}

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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.FileBasedBloomFilter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/** A {@link LookupStoreReader} backed by an {@link SstFileReader}. */
// SortLookupStoreReader 是一个关键的物理层读取组件。
// 它实现了 LookupStoreReader 接口，专门用于读取经过排序并持久化的 SST（Sorted String Table）格式文件。
// 于 SST 文件结构提供高效的点查（Lookup）和迭代查询能力。
// 它将底层存储的多个复杂组件（索引、布隆过滤器、数据块缓存）整合在一起，形成一个统一的查询入口。
// 其内部通过 SstFileReader 来执行具体的查找算法，利用 SST 文件的有序性进行二分查找。
public class SortLookupStoreReader implements LookupStoreReader {
    // 底层文件系统的输入流，支持随机访问（Seek）。
    // 负责从磁盘物理读取字节数据。由于 SST 文件的元数据（Footer）和索引分布在文件末尾或不同块中，必须使用支持随机定位的流。
    private final SeekableInputStream input;
    // 核心读取引擎。
    // 封装了具体的查找逻辑。它持有布隆过滤器（快速排除不存在的键）和索引块（精确定位数据块），是实际执行 lookup 操作的类。
    private final SstFileReader reader;

    public SortLookupStoreReader(
            Comparator<MemorySlice> comparator,
            Path filePath,
            long fileLen,
            SeekableInputStream input,
            CacheManager cacheManager) {
        this.input = input;
        BlockCache blockCache = new BlockCache(filePath, input, cacheManager);

        int footerLen = SortLookupStoreFooter.ENCODED_LENGTH;
        // SST 文件的元数据存储在文件最后（长度固定为 SortLookupStoreFooter.ENCODED_LENGTH）
        MemorySegment footerData =
                blockCache.getBlock(fileLen - footerLen, footerLen, b -> b, true);
        // 解析 SortLookupStoreFooter
        // 从中获取 Index Block（索引块） 和 Bloom Filter（布隆过滤器） 的句柄（位置和长度）。
        SortLookupStoreFooter footer =
                SortLookupStoreFooter.readFooter(MemorySlice.wrap(footerData).toInput());
        FileBasedBloomFilter bloomFilter =
                FileBasedBloomFilter.create(
                        input, filePath, cacheManager, footer.getBloomFilterHandle());
        this.reader =
                new SstFileReader(
                        comparator, blockCache, footer.getIndexBlockHandle(), bloomFilter);
    }
    // 根据 Key 的字节数组查找对应的 Value。
    @Nullable
    @Override
    public byte[] lookup(byte[] key) throws IOException {
        return reader.lookup(key);
    }

    public SstFileReader.SstFileIterator createIterator() {
        return reader.createIterator();
    }

    @Override
    public void close() throws IOException {
        reader.close();
        input.close();
    }
}

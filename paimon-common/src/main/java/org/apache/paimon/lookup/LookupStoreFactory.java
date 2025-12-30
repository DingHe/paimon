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

package org.apache.paimon.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.SortLookupStoreFactory;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

/**
 * A key-value store for lookup, key-value store should be single binary file written once and ready
 * to be used. This factory provide two interfaces:
 *
 * <ul>
 *   <li>Writer: written once to prepare binary file.
 *   <li>Reader: lookup value by key bytes.
 * </ul>
 */
// 在 Apache Paimon 的 lookup（维表查询/查找）逻辑中，LookupStoreFactory 是一个顶层的工厂接口。
// 它负责定义和创建用于一次性写入、多次读取的键值存储组件。
// LookupStoreFactory 的作用是：统一管理 Lookup 存储结构（如 SST 格式）的生命周期。
// 在 Paimon 开启 lookup 功能时（例如在流式 Join 或 Merge 中加速查找），系统需要将某些数据索引化。这个接口确保了无论底层是基于排序的存储（Sort-based）还是其他可能的存储，都能通过一致的接口提供：
// Writer（写入器）：用于将内存中的键值对持久化为一个只读的二进制文件。
// Reader（读取器）：用于从该二进制文件中根据 Key 快速检索 Value。
public interface LookupStoreFactory {
    // 创建一个写入器实例。
    // file：指定要生成的本地二进制文件的路径。
    // bloomFilter：可选的布隆过滤器构建器。如果传入，Writer 会在构建数据块的同时生成布隆过滤器索引。
    LookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException;
    // 创建一个读取器实例。
    // file：要读取的物理文件。
    LookupStoreReader createReader(File file) throws IOException;
    // 生成一个“布隆过滤器生成函数”。
    // 检查配置中是否启用了布隆过滤器（LOOKUP_CACHE_BLOOM_FILTER_ENABLED）
    static Function<Long, BloomFilter.Builder> bfGenerator(Options options) {
        Function<Long, BloomFilter.Builder> bfGenerator = rowCount -> null;
        if (options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_ENABLED)) {
            double bfFpp = options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_FPP);
            bfGenerator =
                    rowCount -> {
                        if (rowCount > 0) {
                            return BloomFilter.builder(rowCount, bfFpp);
                        }
                        return null;
                    };
        }
        return bfGenerator;
    }
    // 用于实例化具体的工厂实现类。
    static LookupStoreFactory create(
            CoreOptions options, CacheManager cacheManager, Comparator<MemorySlice> keyComparator) {
        CompressOptions compression = options.lookupCompressOptions();
        return new SortLookupStoreFactory(
                keyComparator, cacheManager, options.cachePageSize(), compression);
    }

    /** Context between writer and reader. */
    interface Context {}
}

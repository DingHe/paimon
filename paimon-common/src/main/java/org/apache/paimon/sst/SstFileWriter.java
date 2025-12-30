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

package org.apache.paimon.sst;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.memory.MemorySegmentUtils.allocateReuseBytes;
import static org.apache.paimon.sst.BlockHandle.writeBlockHandle;
import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * The writer for writing SST Files. SST Files are row-oriented and designed to serve frequent point
 * queries and range queries by key.
 */
// Apache Paimon 中用于构建 SST (Sorted String Table) 文件的核心引擎。
// 它是一个面向行的写入器，专门为高性能的点查（Point Query）和范围查询（Range Query）设计
// SstFileWriter 的职责是将有序的键值对（Key-Value）转换成符合 SST 规范的物理文件。其核心逻辑包括：
// 分块管理：将数据切分为固定大小的 Data Block。
// 索引构建：自动记录每个数据块的最大 Key 和偏移量，生成 Index Block。
// 数据压缩：在数据块落盘前进行透明压缩（如 LZ4, ZSTD）。
// 完整性校验：为每个块计算 CRC32 校验码，防止数据损坏。
// 过滤加速：集成布隆过滤器（Bloom Filter）以减少不必要的磁盘 IO。
public class SstFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileWriter.class.getName());
    // 魔数，用于标识这是一个有效的 Paimon SST 文件。
    public static final int MAGIC_NUMBER = 1481571681;
    // 文件输出流，支持获取当前写入的物理位置。
    private final PositionOutputStream out;
    // 目标块大小（默认 32KB/64KB）。当 dataBlockWriter 内存占用超过此值时触发 flush。
    private final int blockSize;
    // 数据块缓冲区。临时存放尚未落盘的 Key-Value 数据。
    private final BlockWriter dataBlockWriter;
    // 索引块缓冲区。存放 (每个数据块最大Key -> 该块的位置句柄)。
    private final BlockWriter indexBlockWriter;
    // 布隆过滤器构建器，在 put 时同步记录 Key 的哈希值。
    @Nullable private final BloomFilter.Builder bloomFilter;
    // 使用的压缩算法类型
    private final BlockCompressionType compressionType;
    // 实际执行压缩操作的压缩器实例。
    @Nullable private final BlockCompressor blockCompressor;
    // 记录当前数据块中最后一个（即最大的）Key，用于填充索引。
    private byte[] lastKey;
    // 累计写入的总记录条数。
    private long recordCount;
    // 统计所有块在压缩前的原始大小。
    private long totalUncompressedSize;
    // 统计所有块在压缩后的实际存储大小。
    private long totalCompressedSize;

    public SstFileWriter(
            PositionOutputStream out,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.out = out;
        this.blockSize = blockSize;
        this.dataBlockWriter = new BlockWriter((int) (blockSize * 1.1));
        int expectedNumberOfBlocks = 1024;
        this.indexBlockWriter =
                new BlockWriter(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks);
        this.bloomFilter = bloomFilter;
        if (compressionFactory == null) {
            this.compressionType = BlockCompressionType.NONE;
            this.blockCompressor = null;
        } else {
            this.compressionType = compressionFactory.getCompressionType();
            this.blockCompressor = compressionFactory.getCompressor();
        }
    }

    /**
     * Put the serialized key and value into this SST File. The caller must guarantee that the input
     * key is monotonically incremental according to {@link SstFileReader}'s comparator. Otherwise,
     * the lookup and range query result will be undefined.
     *
     * @param key serialized key
     * @param value serialized value
     */
    // 写入一条 KV 记录。
    public void put(byte[] key, byte[] value) throws IOException {
        // 将数据添加到 dataBlockWriter 缓存
        dataBlockWriter.add(key, value);
        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }

        lastKey = key;
        // 检查 dataBlockWriter 占用的内存。如果超过了设定的 blockSize，自动调用 flush() 将当前块写出。
        if (dataBlockWriter.memory() > blockSize) {
            flush();
        }

        recordCount++;
    }
    // 将当前的 dataBlockWriter 内容作为一个物理块写出。
    public void flush() throws IOException {
        if (dataBlockWriter.size() == 0) {
            return;
        }

        BlockHandle blockHandle = writeBlock(dataBlockWriter);
        MemorySlice handleEncoding = writeBlockHandle(blockHandle);
        // 记录索引快的内容
        indexBlockWriter.add(lastKey, handleEncoding.copyBytes());
    }
    // 任务是将内存中构建好的 Block（数据块） 进行压缩、计算校验和，并最终持久化到磁盘
    private BlockHandle writeBlock(BlockWriter blockWriter) throws IOException {
        // close the block
        // 将当前正在构建的 Block（如 Data Block 或 Index Block）进行封包，返回一个包含原始数据的内存切片 MemorySlice
        MemorySlice block = blockWriter.finish();

        totalUncompressedSize += block.length();

        // attempt to compress the block
        BlockCompressionType blockCompressionType = BlockCompressionType.NONE;
        if (blockCompressor != null) {
            // // 计算压缩后可能的最大长度，并分配重用缓冲区
            int maxCompressedSize = blockCompressor.getMaxCompressedSize(block.length());
            byte[] compressed = allocateReuseBytes(maxCompressedSize + 5);
            // 在压缩数据头部编码原始数据长度（用于解压）
            int offset = encodeInt(compressed, 0, block.length());
            //  执行真正的压缩操作
            int compressedSize =
                    offset
                            + blockCompressor.compress(
                                    block.getHeapMemory(),
                                    block.offset(),
                                    block.length(),
                                    compressed,
                                    offset);

            // Don't use the compressed data if compressed less than 12.5%,
            //  如果压缩率低于 12.5% (1/8)，则放弃压缩
            if (compressedSize < block.length() - (block.length() / 8)) {
                block = new MemorySlice(MemorySegment.wrap(compressed), 0, compressedSize);
                blockCompressionType = this.compressionType;
            }
        }

        totalCompressedSize += block.length();

        // create block trailer
        //  创建 Block 尾部（包含压缩类型和循环冗余校验码 CRC）
        BlockTrailer blockTrailer =
                new BlockTrailer(blockCompressionType, crc32c(block, blockCompressionType));
        MemorySlice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // create a handle to this block
        //  获取当前文件指针位置（偏移量）和数据块长度
        BlockHandle blockHandle = new BlockHandle(out.getPos(), block.length());

        // write data
        // 写入数据内容
        writeSlice(block);

        // write trailer: 5 bytes
        // 写入 5 字节的物理尾部 (1字节压缩类型 + 4字节CRC)
        writeSlice(trailer);

        // clean up state
        // // 清理 BlockWriter 状态以便复用写入下一个 Block
        blockWriter.reset();

        return blockHandle;
    }

    @Nullable
    public BloomFilterHandle writeBloomFilter() throws IOException {
        if (bloomFilter == null) {
            return null;
        }
        MemorySegment buffer = bloomFilter.getBuffer();
        BloomFilterHandle bloomFilterHandle =
                new BloomFilterHandle(out.getPos(), buffer.size(), bloomFilter.expectedEntries());
        writeSlice(MemorySlice.wrap(buffer));
        LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
        return bloomFilterHandle;
    }

    @Nullable
    public BlockHandle writeIndexBlock() throws IOException {
        BlockHandle indexBlock = writeBlock(indexBlockWriter);
        LOG.info("Number of record: {}", recordCount);
        LOG.info("totalUncompressedSize: {}", MemorySize.ofBytes(totalUncompressedSize));
        LOG.info("totalCompressedSize: {}", MemorySize.ofBytes(totalCompressedSize));
        return indexBlock;
    }

    public void writeSlice(MemorySlice slice) throws IOException {
        out.write(slice.getHeapMemory(), slice.offset(), slice.length());
    }
}

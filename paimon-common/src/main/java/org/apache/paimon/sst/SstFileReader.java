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
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An SST File Reader which serves point queries and range queries. Users can call {@code
 * createIterator} to create a file iterator and then use seek and read methods to do range queries.
 *
 * <p>Note that this class is NOT thread-safe.
 */
// 在 Apache Paimon 的存储层中，SstFileReader 是一个至关重要的组件。它模仿了 RocksDB 等 LSM-Tree 存储引擎的设计，
// 专门用于读取 SST (Sorted String Table) 格式的文件。
// 主要作用是提供对有序持久化文件（SST）的高效查询能力。它支持两种查询模式：
// 点查询 (Point Query)：快速查找一个特定的 Key 对应的 Value。
// 范围查询 (Range Query)：通过迭代器定位到某个起始位置，并按顺序读取后续记录。
// 为了实现高性能，它集成了 布隆过滤器 (Bloom Filter) 以减少无效 IO，并使用 两级索引结构（索引块 + 数据块）来定位数据。
public class SstFileReader implements Closeable {
    // 字节比较器。
    private final Comparator<MemorySlice> comparator;
    // 块缓存管理器。
    private final BlockCache blockCache;
    // 索引块读取器。
    // 存储了每个数据块（Data Block）最后一条记录的 Key 以及该块在文件中的位置。它是定位数据的第一站。
    private final BlockReader indexBlock;
    // 基于文件的布隆过滤器。
    // 可选组件。用于在真正读取索引之前判定 Key 是否可能存在，从而极大地优化“查无此记录”的场景。
    @Nullable private final FileBasedBloomFilter bloomFilter;

    public SstFileReader(
            Comparator<MemorySlice> comparator,
            BlockCache blockCache,
            BlockHandle indexBlockHandle,
            @Nullable FileBasedBloomFilter bloomFilter) {
        this.comparator = comparator;
        this.blockCache = blockCache;
        this.indexBlock = readBlock(indexBlockHandle, true);
        this.bloomFilter = bloomFilter;
    }

    /**
     * Lookup the specified key in the file.
     *
     * @param key serialized key
     * @return corresponding serialized value, null if not found.
     */
    @Nullable
    public byte[] lookup(byte[] key) throws IOException {
        // 布隆过滤: 先算 Hash，如果布隆过滤器说没有，直接返回 null。
        if (bloomFilter != null && !bloomFilter.testHash(MurmurHashUtils.hashBytes(key))) {
            return null;
        }
        // 索引定位: 在 indexBlock 中执行 seekTo，找到该 Key 可能存在的那个数据块。
        MemorySlice keySlice = MemorySlice.wrap(key);
        // seek the index to the block containing the key
        BlockIterator indexBlockIterator = indexBlock.iterator();
        indexBlockIterator.seekTo(keySlice);

        // if indexIterator does not have a next, it means the key does not exist in this iterator
        if (indexBlockIterator.hasNext()) {
            // seek the current iterator to the key
            // 数据块查询: 加载对应的数据块（Data Block），在块内查找。如果找到则返回 Value。
            BlockIterator current = getNextBlock(indexBlockIterator);
            if (current.seekTo(keySlice)) {
                return current.next().getValue().copyBytes();
            }
        }
        return null;
    }

    public SstFileIterator createIterator() {
        return new SstFileIterator(indexBlock.iterator());
    }

    private BlockIterator getNextBlock(BlockIterator indexBlockIterator) {
        // index block handle, point to the key, value position.
        MemorySlice blockHandle = indexBlockIterator.next().getValue();
        BlockReader dataBlock =
                readBlock(BlockHandle.readBlockHandle(blockHandle.toInput()), false);
        return dataBlock.iterator();
    }

    /**
     * @param blockHandle The block handle.
     * @param index Whether read the block as an index.
     * @return The reader of the target block.
     */
    private BlockReader readBlock(BlockHandle blockHandle, boolean index) {
        // read block trailer
        MemorySegment trailerData =
                blockCache.getBlock(
                        blockHandle.offset() + blockHandle.size(),
                        BlockTrailer.ENCODED_LENGTH,
                        b -> b,
                        true);
        BlockTrailer blockTrailer =
                BlockTrailer.readBlockTrailer(MemorySlice.wrap(trailerData).toInput());

        MemorySegment unCompressedBlock =
                blockCache.getBlock(
                        blockHandle.offset(),
                        blockHandle.size(),
                        bytes -> decompressBlock(bytes, blockTrailer),
                        index);
        return BlockReader.create(MemorySlice.wrap(unCompressedBlock), comparator);
    }

    private byte[] decompressBlock(byte[] compressedBytes, BlockTrailer blockTrailer) {
        MemorySegment compressed = MemorySegment.wrap(compressedBytes);
        int crc32cCode = crc32c(compressed, blockTrailer.getCompressionType());
        checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                String.format(
                        "Expected CRC32C(%d) but found CRC32C(%d)",
                        blockTrailer.getCrc32c(), crc32cCode));

        // decompress data
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(blockTrailer.getCompressionType());
        if (compressionFactory == null) {
            return compressedBytes;
        } else {
            MemorySliceInput compressedInput = MemorySlice.wrap(compressed).toInput();
            byte[] uncompressed = new byte[compressedInput.readVarLenInt()];
            BlockDecompressor decompressor = compressionFactory.getDecompressor();
            int uncompressedLength =
                    decompressor.decompress(
                            compressed.getHeapMemory(),
                            compressedInput.position(),
                            compressedInput.available(),
                            uncompressed,
                            0);
            checkArgument(uncompressedLength == uncompressed.length);
            return uncompressed;
        }
    }

    @Override
    public void close() throws IOException {
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        blockCache.close();
    }

    /** An Iterator for range queries. */
    public class SstFileIterator {

        private final BlockIterator indexIterator;
        private @Nullable BlockIterator seekedDataBlock = null;

        SstFileIterator(BlockIterator indexBlockIterator) {
            this.indexIterator = indexBlockIterator;
        }

        /**
         * Seek to the position of the record whose key is exactly equal to or greater than the
         * specified key.
         */
        public void seekTo(byte[] key) {
            MemorySlice keySlice = MemorySlice.wrap(key);

            indexIterator.seekTo(keySlice);
            if (indexIterator.hasNext()) {
                seekedDataBlock = getNextBlock(indexIterator);
                // The index block entry key is the last key of the corresponding data block.
                // If there is some index entry key >= targetKey, the related data block must
                // also contain some key >= target key, which means seekedDataBlock.hasNext()
                // must be true
                seekedDataBlock.seekTo(keySlice);
                Preconditions.checkState(seekedDataBlock.hasNext());
            } else {
                seekedDataBlock = null;
            }
        }

        /**
         * Read a batch of records from this SST File and move current record position to the next
         * batch.
         *
         * @return current batch of records, null if reaching file end.
         */
        public BlockIterator readBatch() throws IOException {
            if (seekedDataBlock != null) {
                BlockIterator result = seekedDataBlock;
                seekedDataBlock = null;
                return result;
            }

            if (!indexIterator.hasNext()) {
                return null;
            }

            return getNextBlock(indexIterator);
        }
    }
}

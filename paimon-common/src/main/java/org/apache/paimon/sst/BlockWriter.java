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

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.utils.IntArrayList;

import java.io.IOException;

import static org.apache.paimon.sst.BlockAlignedType.ALIGNED;
import static org.apache.paimon.sst.BlockAlignedType.UNALIGNED;

/**
 * Writer to build a Block. A block is designed for storing and random-accessing k-v pairs. The
 * layout is as below:
 *
 * <pre>
 *     +---------------+
 *     | Block Trailer |
 *     +------------------------------------------------+
 *     |       Block CRC23C      |     Compression      |
 *     +------------------------------------------------+
 *     +---------------+
 *     |  Block Data   |
 *     +---------------+--------------------------------+----+
 *     | key len | key bytes | value len | value bytes  |    |
 *     +------------------------------------------------+    |
 *     | key len | key bytes | value len | value bytes  |    +-> Key-Value pairs
 *     +------------------------------------------------+    |
 *     |                  ... ...                       |    |
 *     +------------------------------------------------+----+
 *     | entry pos | entry pos |     ...    | entry pos |    +-> optional, for unaligned block
 *     +------------------------------------------------+----+
 *     |   entry num  /  entry size   |   aligned type  |
 *     +------------------------------------------------+
 * </pre>
 */
// 在 Apache Paimon 的 LSM 存储引擎中，BlockWriter 是构建 SST (Sorted String Table) 文件的基础组件。
// 它的主要任务是将一个个键值对（Key-Value pairs）组织成一个有序且可随机访问的内存块（Block）。
// BlockWriter 的作用是在内存中构建符合特定布局的数据块。其设计目标是平衡存储密度与读取性能。
// KV 序列化：将键和值的长度及其二进制数据紧凑地写入内存。
// 支持随机访问：通过记录每个 Entry 的位置（Position），支持在 Block 内部进行二分查找。
// 对齐模式 (Aligned)：如果 Block 内所有记录的大小完全一致，则不存储每个记录的偏移量，仅记录单条大小，从而节省空间。
// 非对齐模式 (Unaligned)：如果记录大小不一，则在 Block 末尾存储一个偏移量数组（Entry Positions），方便索引。


public class BlockWriter {
    // 动态数组，
    // 用于记录当前 Block 中每一个 Entry（记录）在内存缓冲区中的起始偏移量（Start Position）
    // 在非对齐模式下，这些偏移量会被写入 Block 末尾，作为查找索引。
    private final IntArrayList positions;
    // 实际存储数据的内存缓冲区。
    private final MemorySliceOutput block;
    // 记录对齐模式下的固定条目大小。
    private int alignedSize;
    // 如果在添加过程中发现新记录的大小与 alignedSize 不一致，则永久切换为 false（非对齐）。
    private boolean aligned;

    public BlockWriter(int blockSize) {
        this.positions = new IntArrayList(32);
        this.block = new MemorySliceOutput(blockSize + 128);
        this.alignedSize = 0;
        this.aligned = true;
    }

    public void reset() {
        this.positions.clear();
        this.block.reset();
        this.alignedSize = 0;
        this.aligned = true;
    }
    // 向当前 Block 添加一个键值对。
    public void add(byte[] key, byte[] value) {
        int startPosition = block.size();
        // 写入 key.length（变长整数）和 key 字节数组。
        block.writeVarLenInt(key.length);
        block.writeBytes(key);
        // 写入 value.length（变长整数）和 value 字节数组。
        block.writeVarLenInt(value.length);
        block.writeBytes(value);
        int endPosition = block.size();

        positions.add(startPosition);
        if (aligned) {
            // 计算这条记录的总长度（endPosition - startPosition）
            int currentSize = endPosition - startPosition;
            if (alignedSize == 0) {
                alignedSize = currentSize;
            } else {
                aligned = alignedSize == currentSize;
            }
        }
    }

    public int size() {
        return positions.size();
    }
    // 估算当前 Block 已经占用的总内存大小（字节）。
    public int memory() {
        int memory = block.size() + 5;
        if (!aligned) {
            memory += positions.size() * 4;
        }
        return memory;
    }
    // 封装并结束 Block 的构建，返回最终的内存切片。
    public MemorySlice finish() throws IOException {
        if (positions.isEmpty()) {
            // Do not use alignment mode, as it is impossible to calculate how many records are
            // inside when reading
            aligned = false;
        }
        // 如果 aligned 为 true：在末尾写入 4 字节的 alignedSize。
        if (aligned) {
            block.writeInt(alignedSize);
        } else {
            // 如果 aligned 为 false：依次写入所有 positions 偏移量，最后写入 4 字节的记录总数。
            for (int i = 0; i < positions.size(); i++) {
                block.writeInt(positions.get(i));
            }
            block.writeInt(positions.size());
        }
        // 写入标志位：最后写入 1 字节记录当前的对齐类型（ALIGNED 或 UNALIGNED）
        block.writeByte(aligned ? ALIGNED.toByte() : UNALIGNED.toByte());
        return block.toSlice();
    }
}

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

package org.apache.paimon.reader;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The reader that reads the batches of records.
 *
 * @since 0.4.0
 */
// RecordReader 是最底层的数据拉取接口。它负责从物理存储（如 Parquet、ORC 文件）中批量提取原始数据，并将其转化为内存对象。
// RecordReader 的核心作用是实现向量化批量读取（Batch Reading）。 与传统的 Iterator（一次只拿一条数据）不同，Paimon 为了提高 IO 效率和减少函数调用开销，采用了“批读取”模式。
// 批量拉取：每次调用 readBatch() 返回一个 RecordIterator，这个迭代器内部包含了一组记录（通常是几千行）。
// 内存复用：它允许在批次处理完后调用 releaseBatch()，这给底层提供了一个信号，以便回收或复用昂贵的内存结构（如 Columnar Batch）。
// 链式操作：提供了 transform 和 filter 方法，允许在数据从磁盘读入内存的过程中直接进行转换或过滤。
// 在 Apache Paimon 中，RecordReader 的实现类非常丰富，因为 Paimon 需要处理多种表类型（主键表 vs. 追加表）、多种文件格式（Parquet/ORC/Avro）以及多种合并引擎。
@Public
public interface RecordReader<T> extends Closeable {

    /**
     * Reads one batch. The method should return null when reaching the end of the input.
     *
     * <p>The returned iterator object and any contained objects may be held onto by the source for
     * some time, so it should not be immediately reused by the reader.
     */
    // 获取下一批数据。
    @Nullable
    RecordIterator<T> readBatch() throws IOException;

    /** Closes the reader and should release all resources. */
    // 关闭读取器。
    @Override
    void close() throws IOException;

    /**
     * An internal iterator interface which presents a more restrictive API than {@link Iterator}.
     */
    interface RecordIterator<T> {

        /**
         * Gets the next record from the iterator. Returns null if this iterator has no more
         * elements.
         */
        @Nullable
        T next() throws IOException;

        /**
         * Releases the batch that this iterator iterated over. This is not supposed to close the
         * reader and its resources, but is simply a signal that this iterator is not used anymore.
         * This method can be used as a hook to recycle/reuse heavyweight object structures.
         */
        void releaseBatch();

        /** Returns an iterator that applies {@code function} to each element. */
        default <R> RecordReader.RecordIterator<R> transform(Function<T, R> function) {
            RecordReader.RecordIterator<T> thisIterator = this;
            return new RecordReader.RecordIterator<R>() {
                @Nullable
                @Override
                public R next() throws IOException {
                    T next = thisIterator.next();
                    if (next == null) {
                        return null;
                    }
                    return function.apply(next);
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }

        /** Filters a {@link RecordIterator}. */
        default RecordIterator<T> filter(Filter<T> filter) {
            RecordIterator<T> thisIterator = this;
            return new RecordIterator<T>() {
                @Nullable
                @Override
                public T next() throws IOException {
                    while (true) {
                        T next = thisIterator.next();
                        if (next == null) {
                            return null;
                        }
                        if (filter.test(next)) {
                            return next;
                        }
                    }
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }
    }

    // -------------------------------------------------------------------------
    //                     Util methods
    // -------------------------------------------------------------------------

    /**
     * Performs the given action for each remaining element in {@link RecordReader} until all
     * elements have been processed or the action throws an exception.
     */
    // 消费剩余所有数据。
    // 它封装了双重循环（外层循环读批次，内层循环读记录），并确保在最后自动执行 close()。这是用户最常用的高层 API。
    default void forEachRemaining(Consumer<? super T> action) throws IOException {
        RecordReader.RecordIterator<T> batch;
        T record;

        try {
            while ((batch = readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /**
     * Performs the given action for each remaining element with row position in {@link
     * RecordReader} until all elements have been processed or the action throws an exception.
     */
    // 消费数据并获取行在文件中的物理位置（Row Position）
    // 通常用于需要行号追踪的场景（如审计日志或索引映射）
    default void forEachRemainingWithPosition(BiConsumer<Long, ? super T> action)
            throws IOException {
        FileRecordIterator<T> batch;
        T record;

        try {
            while ((batch = (FileRecordIterator<T>) readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(batch.returnedPosition(), record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /** Returns a {@link RecordReader} that applies {@code function} to each element. */
    // 返回一个新的 RecordReader<R>，在读取每一批时自动应用转换函数。
    default <R> RecordReader<R> transform(Function<T, R> function) {
        RecordReader<T> thisReader = this;
        return new RecordReader<R>() {
            @Nullable
            @Override
            public RecordIterator<R> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.transform(function);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Filters a {@link RecordReader}. */
    // 返回一个新的读取器，只返回符合条件的记录。
    default RecordReader<T> filter(Filter<T> filter) {
        RecordReader<T> thisReader = this;
        return new RecordReader<T>() {
            @Nullable
            @Override
            public RecordIterator<T> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.filter(filter);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Convert this reader to a {@link CloseableIterator}. */
    // 将复杂的“批读取”接口包装成标准的 Java CloseableIterator，方便集成到标准的 Java 集合框架或 Stream 中。
    default CloseableIterator<T> toCloseableIterator() {
        return new RecordReaderIterator<>(this);
    }
}

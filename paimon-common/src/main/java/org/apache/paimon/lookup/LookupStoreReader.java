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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

/** Reader, lookup value by key bytes. */
// 根据键（Key）的字节数组，从存储介质中快速检索其对应的值（Value）的字节数组。
// 它是 Paimon Lookup 索引或 点查存储 的抽象。在 Paimon 中，当表配置了 lookup 索引（用于加速 Merge 或 Join）时，系统会将数据存储在类似 RocksDB 或 Paimon 自研的 SST 文件结构中。
// 该接口封装了“从这些结构中根据 Key 查找 Value”的动作，屏蔽了底层具体的存储实现细节。
public interface LookupStoreReader extends Closeable {

    /** Lookup value by key. */
    // byte[] key: 需要查找的键的原始字节数组。
    // 如果找到了该 Key，则返回对应的 Value 字节数组。
    @Nullable
    byte[] lookup(byte[] key) throws IOException;
}

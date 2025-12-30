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

import java.io.Closeable;
import java.io.IOException;

/** Writer to prepare binary file. */
// 将格式化后的键值对（Key-Value）持久化到高性能的本地索引文件中（通常是 SST 格式，即 Sorted String Table）
public interface LookupStoreWriter extends Closeable {

    /** Put key value to store. */
    void put(byte[] key, byte[] value) throws IOException;
}

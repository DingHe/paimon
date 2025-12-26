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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.Public;

/**
 * Kind of a file.
 *
 * @since 0.9.0
 */
// FileKind 的主要作用是实现 LSM-Tree（分层存储）架构下的增量元数据管理。
// 在 Paimon 的元数据文件中，并不仅仅记录当前有哪些文件，而是记录了一系列“变更”。FileKind 标明了某次提交对某个数据文件做了什么操作：
// 版本演进：通过记录文件的添加（ADD）和删除（DELETE），Paimon 可以从一个快照平滑地推演到下一个快照。
// 读时合并：在读取数据时，系统会根据 FileKind 过滤掉那些已经被标记为删除的文件。
// 压缩支持：在 Compaction（文件合并/压缩）过程中，旧的小文件会被标记为 DELETE，而合并后的新文件会被标记为 ADD。
@Public
public enum FileKind {
    // 表示该操作向表中引入了一个新的数据文件。
    // 场景：正常的写入（Append）、覆盖写入（Overwrite）后的新文件、或者 Compaction 产生的新文件。
    ADD((byte) 0),
    // 表示该操作从逻辑上删除了一个已存在的数据文件。
    // 场景：Compaction 清理掉的旧文件、或者在 OVERWRITE 模式下被替换掉的老版本文件。
    DELETE((byte) 1);

    private final byte value;

    FileKind(byte value) {
        this.value = value;
    }

    public byte toByteValue() {
        return value;
    }

    public static FileKind fromByteValue(byte value) {
        switch (value) {
            case 0:
                return ADD;
            case 1:
                return DELETE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for value kind.");
        }
    }
}

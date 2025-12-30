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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Common implementation of {@link CompactRewriter}. */
// 核心作用是处理合并过程中的非重写任务
// 层级升级（Upgrade）逻辑：定义了如何不经过物理重写直接提升文件的层级。
// 元数据提取：提供工具方法，从复杂的合并结构（Sections）中提取出原始文件的元数据，以便记录“合并前（Before）”的文件集合。
public abstract class AbstractCompactRewriter implements CompactRewriter {
    // 将单个数据文件提升到目标层级，而无需重新读取和写入数据。
    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        return new CompactResult(file, file.upgrade(outputLevel));
    }
    // 用于从“待合并段（Sections）”结构中提取出所有的原始文件元数据。
    protected static List<DataFileMeta> extractFilesFromSections(List<List<SortedRun>> sections) {
        return sections.stream()
                .flatMap(Collection::stream)
                .map(SortedRun::files)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {}
}

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

package org.apache.paimon.table;

import org.apache.paimon.options.ExpireConfig;
// 由于 Paimon 采用的是基于 Snapshot（快照） 的多版本并发控制（MVCC）架构，每一次写入、合并（Compaction）或删除操作都会生成一个新的快照。
// 如果不对快照进行管理，会产生两个主要问题：
// 存储空间爆炸：旧快照引用的数据文件会一直保留在磁盘上，占用大量空间。
// 查询与维护性能下降：元数据（Manifest 文件）过多会导致读取 Snapshot List 变慢。
// ExpireSnapshots 接口的作用就是：根据预设策略，逻辑上废弃不再需要的快照，
// 并物理上删除那些不再被任何存活快照引用的旧数据文件（Data Files）和元数据文件（Manifests）。
/** Expire snapshots. */
public interface ExpireSnapshots {
    // 为过期任务注入具体的参数策略
    ExpireSnapshots config(ExpireConfig expireConfig);
    // 触发实际的过期清理操作。
    /** @return How many snapshots have been expired. */
    int expire();
}

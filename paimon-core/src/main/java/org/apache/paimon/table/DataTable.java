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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

/** A {@link Table} for data. */
// 如果说 Table 是基础定义，InnerTable 是内部实现接口，那么 DataTable 就是真正持有物理存储元数据和管理组件的核心入口。
// DataTable 的主要作用是提供对 Paimon 表底层管理组件的全面访问能力。
public interface DataTable extends InnerTable {
    // 创建一个数据表扫描器。
    // 相比于 InnerTable.newScan()，它返回更具体的 DataTableScan。
    // 它不仅能决定读哪些文件，还能处理分区过滤（Partition Pruning）和桶（Bucket）过滤。
    @Override
    DataTableScan newScan();

    SnapshotReader newSnapshotReader();

    CoreOptions coreOptions();

    SnapshotManager snapshotManager();

    ChangelogManager changelogManager();

    ConsumerManager consumerManager();

    SchemaManager schemaManager();

    TagManager tagManager();

    BranchManager branchManager();

    /**
     * Get {@link DataTable} with branch identified by {@code branchName}. Note that this method
     * does not keep dynamic options in current table.
     */
    DataTable switchToBranch(String branchName);

    Path location();
}

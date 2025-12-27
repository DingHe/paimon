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

package org.apache.paimon.options.description;

/** Part of a {@link Description} that can be converted into String representation. */
// 描述内容的最小原子单元
//Paimon 的配置选项（ConfigOption）通常需要详细的文档说明。为了支持不同的输出格式（例如：纯文本、Markdown 表格、HTML），
// Paimon 没有直接将描述写死为固定字符串，而是设计了一套“描述系统”。
interface DescriptionElement {
    /**
     * Transforms itself into String representation using given format.
     *
     * @param formatter formatter to use.
     */
    void format(Formatter formatter);
}

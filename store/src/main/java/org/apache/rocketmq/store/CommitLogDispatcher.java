/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store;

import org.rocksdb.RocksDBException;

/**
 * 负责将commitlog中消息的进行分发的类
 *
 * <p>主要用途有以下几个：
 * <ul>
 *     <li>用于构造：$HOME/store/consumequeue/<topic>/<queueId></li>
 *     <li>用于构造：$HOME/store/index/</li>
 *     <li>过滤数据</li>
 * </ul>
 */
public interface CommitLogDispatcher {

    /**
     * 将在存储中的消息分发用于构建consumequeue、index和过滤数据
     *
     * @param request 用于分发数据的请求
     * @throws RocksDBException 仅在{@link StoreType#DEFAULT_ROCKSDB}时才会出现
     */
    void dispatch(final DispatchRequest request) throws RocksDBException;
}

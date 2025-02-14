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
package org.apache.rocketmq.store.config;

import org.apache.rocketmq.common.mawen.CorePart;
import org.apache.rocketmq.common.mawen.MessageLost;

/**
 * 磁盘刷新类型
 */
@CorePart(value = "生产者发送消息到broker，保存到", part = CorePart.Part.STORE)
public enum FlushDiskType {
    /**
     * 同步刷新
     *
     * <p>每次发送消息后，只有等待消息被写入到磁盘中，才会返回响应。
     */
    SYNC_FLUSH,
    /**
     * 异步刷新
     */
    @MessageLost(reason = "如果在等待下次异步刷新期间，broker或及其宕机，会导致消息出现丢失")
    ASYNC_FLUSH
}

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
package org.apache.rocketmq.common.consumer;

/**
 * 从哪里开始消费
 */
public enum ConsumeFromWhere {
    /**
     * 消费者从上次消费停止的地方继续消费。如果是新启动的客户端，将有两种情况：
     * <ul>
     *     <li>如果消费者组创建时间尚短，最早订阅的消息尚未过期，即该消费者组代表的业务才刚开始启动，则从头消费</li>
     *     <li>如果订阅的最早消息已过期，则从最新消息开始消费，这意味着在启动时间戳之前诞生的消息竟会被忽略</li>
     * </ul>
     */
    CONSUME_FROM_LAST_OFFSET,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,
    /**
     * 消息者将从最早可用的消息开始消费
     */
    CONSUME_FROM_FIRST_OFFSET,
    /**
     * 消费者将从特定时间戳启动，意味着在该时间戳之前生成的消息将会被忽略
     */
    CONSUME_FROM_TIMESTAMP,
}

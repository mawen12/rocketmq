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
package org.apache.rocketmq.client.consumer;

import java.util.List;

import org.apache.rocketmq.common.annotation.ImportantPoint;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 用于在消费者之间分配消息队列的算法，因为在订阅时，仅指定了主题和消息过滤，并未指定消费者能监听主题下哪些队列，因此该方法决定了如何分配消息队列给消费者。
 */

@ImportantPoint("决定了哪些队列中的消息被分配给消费者")
public interface AllocateMessageQueueStrategy {

    /**
     * 根据消费者ID分配
     *
     * @param consumerGroup 客户端的消费者分组
     * @param currentCID 当前消费者ID
     * @param mqAll 当前队列中的消费者队列集合
     * @param cidAll 当前分组下的消费者集合
     * @return 当前客户端
     */
    List<MessageQueue> allocate(final String consumerGroup, final String currentCID, final List<MessageQueue> mqAll, final List<String> cidAll);

    /**
     * 策略名称
     *
     * @return The strategy name
     */
    String getName();
}

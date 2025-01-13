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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * 消费偏移量存储接口，即保存了消费者消费进度的存储
 * <ul>
 *     <li>{@link MessageModel#CLUSTERING}：{@link RemoteBrokerOffsetStore}</li>
 *     <li>{@link MessageModel#BROADCASTING}：{@link LocalFileOffsetStore}</li>
 * </ul>
 */
public interface OffsetStore {
    /**
     * 加载偏移量
     */
    void load() throws MQClientException;

    /**
     * 更新消息队列的消费偏移量，存储在内存中
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * 更新并冻结消息队列的偏移量，阻止并发更新
     *
     * @param mq target message queue
     * @param offset expect update offset
     */
    void updateAndFreezeOffset(final MessageQueue mq, final long offset);

    /**
     * 从本地存储中获取消息队列的消费偏移量
     *
     * @return The fetched offset
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 持久化多个消息队列的消费偏移量，视底层实现存储在本地或者Broker
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * 持久化多个消息队列的消费偏移量，视底层实现存储在本地或者Broker
     */
    void persist(final MessageQueue mq);

    /**
     * 移除消息队列的消费偏移量
     */
    void removeOffset(MessageQueue mq);

    /**
     * @return 返回主题下所有队列的消费偏移量
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 将消息队列的消费偏移量同步到Broker
     *
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
}

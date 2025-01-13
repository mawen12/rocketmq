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
package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;

/**
 * 消息消费服务，{@link org.apache.rocketmq.client.consumer.listener.MessageListener}的底层实现
 */
public interface ConsumeMessageService {
    /**
     * 启动消费服务
     */
    void start();

    /**
     * 等待时间内暂停
     *
     * @param awaitTerminateMillis
     */
    void shutdown(long awaitTerminateMillis);

    /**
     * 更新核心线程数
     *
     * @param corePoolSize
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * 增加核心线程数
     */
    void incCorePoolSize();

    /**
     * 降低核心线程数
     */
    void decCorePoolSize();

    /**
     * 返回核心线程数
     * @return
     */
    int getCorePoolSize();

    /**
     * 返回消费消息结果
     * @param msg
     * @param brokerName
     * @return
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    /**
     * 提交消费请求
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     * @param dispathToConsume
     */
    void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispathToConsume);

    /**
     * 提交POP消费请求
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     */
    void submitPopConsumeRequest(final List<MessageExt> msgs, final PopProcessQueue processQueue, final MessageQueue messageQueue);
}

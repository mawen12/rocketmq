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

package org.apache.rocketmq.client.consumer.listener;

import java.util.List;

/**
 * 消费返回类型，基于消费过程中超时、异常、失败、成功等场景的整体总结
 */
public enum ConsumeReturnType {
    /**
     * 消费监听器返回消费成功
     *
     * @see MessageListenerConcurrently#consumeMessage(List, ConsumeConcurrentlyContext)
     * @see ConsumeConcurrentlyStatus#CONSUME_SUCCESS
     */
    SUCCESS,
    /**
     * 消费超时，无论消费是否成功，超时时间为{@link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#consumeTimeout}，单位为分钟
     */
    TIME_OUT,
    /**
     * 消费过程中抛出异常
     */
    EXCEPTION,
    /**
     * 消费结束返回null，并且未出现异常
     */
    RETURNNULL,
    /**
     * 消费失败，稍后进行重试
     *
     * @see MessageListenerConcurrently#consumeMessage(List, ConsumeConcurrentlyContext)
     * @see ConsumeConcurrentlyStatus#RECONSUME_LATER
     */
    FAILED
}

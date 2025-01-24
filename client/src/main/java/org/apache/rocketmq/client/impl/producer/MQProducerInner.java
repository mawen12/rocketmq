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
package org.apache.rocketmq.client.impl.producer;

import java.util.Set;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

/**
 * 内部的MQ Producer 接口
 */
public interface MQProducerInner {
    /**
     * @return 返回所有的发布主题
     */
    Set<String> getPublishTopicList();

    /**
     * @param topic 主题
     * @return 检查主题信息是否需要更新
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    /**
     * @return 事务消息相关的监听器
     */
    TransactionCheckListener checkListener();

    /**
     * @return 事务消息相关的监听器
     */
    TransactionListener getCheckListener();

    /**
     * 事务消息相关
     *
     * @param addr
     * @param msg
     * @param checkRequestHeader
     */
    void checkTransactionState(final String addr, final MessageExt msg, final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 将主题信息更新到Broker
     *
     * @param topic
     * @param info
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    /**
     * @return 是否测试模式
     */
    boolean isUnitMode();
}

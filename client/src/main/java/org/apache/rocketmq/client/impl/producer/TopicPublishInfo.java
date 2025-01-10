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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/**
 * 主题发布信息，包含消息队列列表，并提供轮询选择，以及基于broker的轮询选择
 */
public class TopicPublishInfo {
    /**
     * 是否为有序主题，即消息是否有序，默认为false，即消息是无序的
     */
    private boolean orderTopic = false;
    /**
     * 是否包含主题路由信息，默认为false，即不包含
     */
    private boolean haveTopicRouterInfo = false;
    /**
     * 该主题下所有的消息队列
     */
    private List<MessageQueue> messageQueueList = new ArrayList<>();
    /**
     * 消息被选择发送的消息队列索引，即基于轮询机制选择的消息队列
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * 主题路由数据
     */
    private TopicRouteData topicRouteData;

    /**
     * 队列过滤器
     */
    public interface QueueFilter {
        /**
         * 返回指定队列是否过滤
         *
         * @param mq
         * @return
         */
        boolean filter(MessageQueue mq);
    }

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    /**
     * 该主题是否准备好接收消息，即有消息队列能存储消息
     *
     * @return
     */
    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public MessageQueue selectOneMessageQueue(QueueFilter ...filter) {
        return selectOneMessageQueue(this.messageQueueList, this.sendWhichQueue, filter);
    }

    /**
     * 从所有满足队列过滤器的消息队列中选择一个
     *
     * @param messageQueueList
     * @param sendQueue
     * @param filter
     * @return
     */
    private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, ThreadLocalIndex sendQueue, QueueFilter ...filter) {
        /**
         * 没有队列，无法选择，直接返回
         */
        if (messageQueueList == null || messageQueueList.isEmpty()) {
            return null;
        }

        if (filter != null && filter.length != 0) {
            for (int i = 0; i < messageQueueList.size(); i++) {
                /**
                 * 获取下一个被选择的消息队列
                 */
                int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
                /**
                 * 获取该索引对应的队列
                 */
                MessageQueue mq = messageQueueList.get(index);
                boolean filterResult = true;
                /**
                 * 该消息队列是否满足过滤条件，如果可以，则代表队列可以被使用；否则跳过这个，选择下一个
                 */
                for (QueueFilter f: filter) {
                    Preconditions.checkNotNull(f);
                    filterResult &= f.filter(mq);
                }
                if (filterResult) {
                    return mq;
                }
            }

            return null;
        }

        /**
         * 直接获取下一个被选择的队列索引
         */
        int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
        /**
         * 获取该索引对应的队列
         */
        return messageQueueList.get(index);
    }

    public void resetIndex() {
        this.sendWhichQueue.reset();
    }

    /**
     * 根据上次的broker名称选择一个队列，即基于broker的轮询
     *
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            /**
             * 没有名称，则使用轮询选择一个
             */
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                MessageQueue mq = selectOneMessageQueue();
                /**
                 * 如果与上一次的broker名称不同，则使用；否则取下一个broker的消息队列
                 */
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            /**
             * 如果均不满足，则使用轮询选择一个
             */
            return selectOneMessageQueue();
        }
    }

    /**
     * 基于轮询机制选择消息队列
     *
     * @return
     */
    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = index % this.messageQueueList.size();

        return this.messageQueueList.get(pos);
    }

    public int getWriteQueueNumsByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}

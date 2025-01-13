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

/**
 * $Id: QueryConsumerOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

/**
 * 查询消费者分组下，指定主题的指定队列的消费进度
 */
@RocketMQAction(value = RequestCode.QUERY_CONSUMER_OFFSET, action = Action.GET)
public class QueryConsumerOffsetRequestHeader extends TopicQueueRequestHeader {
    /**
     * 消费者分组
     */
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;
    /**
     * 消费主题
     */
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;
    /**
     * 队列ID
     */
    @CFNotNull
    private Integer queueId;
    /**
     * 如果消费者分组还没有消费该队列，是否返回0
     */
    private Boolean setZeroIfNotFound;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public Integer getQueueId() {
        return queueId;
    }

    @Override
    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Boolean getSetZeroIfNotFound() {
        return setZeroIfNotFound;
    }

    public void setSetZeroIfNotFound(Boolean setZeroIfNotFound) {
        this.setZeroIfNotFound = setZeroIfNotFound;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("consumerGroup", consumerGroup)
            .add("topic", topic)
            .add("queueId", queueId)
            .add("setZeroIfNotFound", setZeroIfNotFound)
            .toString();
    }
}

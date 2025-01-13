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

package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.RpcRequestHeader;

/**
 * 在消费者消费失败时，由消费者将消息发送回Broker
 *
 * @see
 * @see org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#consumerSendMsgBack(ChannelHandlerContext, RemotingCommand)
 */
@RocketMQAction(value = RequestCode.CONSUMER_SEND_MSG_BACK, action = Action.SUB)
public class ConsumerSendMsgBackRequestHeader extends RpcRequestHeader {
    /**
     * 消息在队列中的偏移量
     */
    @CFNotNull
    private Long offset;
    /**
     * 消息者分组
     */
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String group;
    /**
     * 消息延迟级别，默认为0，即由服务端控制
     */
    @CFNotNull
    private Integer delayLevel;
    /**
     * 消息ID
     */
    private String originMsgId;
    /**
     * 消息所属的主题
     */
    @RocketMQResource(ResourceType.TOPIC)
    private String originTopic;
    @CFNullable
    private boolean unitMode = false;
    /**
     * 消息最大重新消费次数
     */
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(Integer delayLevel) {
        this.delayLevel = delayLevel;
    }

    public String getOriginMsgId() {
        return originMsgId;
    }

    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }

    public String getOriginTopic() {
        return originTopic;
    }

    public void setOriginTopic(String originTopic) {
        this.originTopic = originTopic;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("offset", offset)
            .add("group", group)
            .add("delayLevel", delayLevel)
            .add("originMsgId", originMsgId)
            .add("originTopic", originTopic)
            .add("unitMode", unitMode)
            .add("maxReconsumeTimes", maxReconsumeTimes)
            .toString();
    }
}

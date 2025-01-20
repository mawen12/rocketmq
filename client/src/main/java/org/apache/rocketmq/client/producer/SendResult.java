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
package org.apache.rocketmq.client.producer;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Client发送消息到Broker，Broker返回的结果
 */
public class SendResult {
    /**
     * 发送状态，要么成功，要么消息存储失败
     *
     * <p>该值是响应上的{@link org.apache.rocketmq.remoting.protocol.ResponseCode}
     */
    private SendStatus sendStatus;
    /**
     * 消息ID，由客户端的{@link org.apache.rocketmq.common.message.MessageClientIDSetter#setUniqID(Message)}来设置
     *
     * <p>直接从客户端的消息上获取
     *
     * @see org.apache.rocketmq.common.message.Message#properties[UNIQ_KEY]
     */
    private String msgId;
    /**
     * 存储消息的队列，该消息被存储在ENV(user.home)/store/commitLog中，
     *
     * <p>其中的QueueId是响应上的{@link org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader#queueId}
     */
    private MessageQueue messageQueue;
    /**
     * 队列偏移量
     *
     * <p>是响应中的{@link org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader#queueOffset}
     *
     * <p>是指消息对指定队列中是第几条
     */
    private long queueOffset;
    /**
     * 事务ID，仅在发送事务消息时才有值
     */
    private String transactionId;
    /**
     * 偏移量消息ID
     *
     * <p>是响应上{@link org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader#msgId}
     *
     * <p>是指消息在{@code commitlog}中的偏移量
     */
    private String offsetMsgId;

    private String regionId;
    /**
     * 消息是否可追踪
     */
    private boolean traceOn = true;
    /**
     * 原始的响应体
     */
    private byte[] rawRespBody;

    private String recallHandle;

    public SendResult() {
    }

    public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue,
        long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.offsetMsgId = offsetMsgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }

    public SendResult(final SendStatus sendStatus, final String msgId, final MessageQueue messageQueue,
        final long queueOffset, final String transactionId,
        final String offsetMsgId, final String regionId) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
        this.transactionId = transactionId;
        this.offsetMsgId = offsetMsgId;
        this.regionId = regionId;
    }

    public static String encoderSendResultToJson(final Object obj) {
        return JSON.toJSONString(obj);
    }

    public static SendResult decoderSendResultFromJson(String json) {
        return JSON.parseObject(json, SendResult.class);
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(final boolean traceOn) {
        this.traceOn = traceOn;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getOffsetMsgId() {
        return offsetMsgId;
    }

    public void setOffsetMsgId(String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }

    public String getRecallHandle() {
        return recallHandle;
    }

    public void setRecallHandle(String recallHandle) {
        this.recallHandle = recallHandle;
    }

    @Override
    public String toString() {
        return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ", offsetMsgId=" + offsetMsgId + ", messageQueue=" + messageQueue + ", queueOffset=" + queueOffset + ", recallHandle=" + recallHandle + "]";
    }

    public void setRawRespBody(byte[] body) {
        this.rawRespBody = body;
    }

    public byte[] getRawRespBody() {
        return rawRespBody;
    }
}

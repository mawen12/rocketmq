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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 拉API包装器
 */
public class PullAPIWrapper {
    private static final Logger log = LoggerFactory.getLogger(PullAPIWrapper.class);
    private final MQClientInstance mQClientFactory;
    /**
     * 消费者分组
     */
    private final String consumerGroup;
    private final boolean unitMode;
    /**
     * Map<消息队列, BrokerId>
     */
    private ConcurrentMap<MessageQueue, AtomicLong> pullFromWhichNodeTable = new ConcurrentHashMap<>(32);

    private volatile boolean connectBrokerByUser = false;
    /**
     * 默认的BrokerId为MASTER，brokerId=0
     */
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    /**
     * 用于生成随机数
     */
    private Random random = new Random(System.nanoTime());

    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodesBatch(byteBuffer, this.mQClientFactory.getClientConfig().isDecodeReadBody(), this.mQClientFactory.getClientConfig().isDecodeDecompressBody(), true);

            boolean needDecodeInnerMessage = false;
            for (MessageExt messageExt: msgList) {
                if (MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) && MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.NEED_UNWRAP_FLAG)) {
                    needDecodeInnerMessage = true;
                    break;
                }
            }
            if (needDecodeInnerMessage) {
                List<MessageExt> innerMsgList = new ArrayList<>();
                try {
                    for (MessageExt messageExt: msgList) {
                        if (MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) && MessageSysFlag.check(messageExt.getSysFlag(), MessageSysFlag.NEED_UNWRAP_FLAG)) {
                            MessageDecoder.decodeMessage(messageExt, innerMsgList);
                        } else {
                            innerMsgList.add(messageExt);
                        }
                    }
                    msgList = innerMsgList;
                } catch (Throwable t) {
                    log.error("Try to decode the inner batch failed for {}", pullResult.toString(), t);
                }
            }

            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
                msg.setQueueId(mq.getQueueId());
                if (pullResultExt.getOffsetDelta() != null) {
                    msg.setQueueOffset(pullResultExt.getOffsetDelta() + msg.getQueueOffset());
                }
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    public PullResult pullKernelImpl(final MessageQueue mq, final String subExpression, final String expressionType, final long subVersion, final long offset,
                                     final int maxNums, final int maxSizeInBytes, final int sysFlag, final long commitOffset, final long brokerSuspendMaxTimeMillis,
                                     final long timeoutMillis, final CommunicationMode communicationMode, final PullCallback pullCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 从内存中读取Broker信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            // 从Namesrv读取Broker的信息，并更新到内存中
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            // 从内存中再次读取
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            // 校验版本
            {
                if (!ExpressionType.isTagType(expressionType) && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    // V4_1_0_SNAPSHOT之前的版本，仅支持基于Tag类型的过滤，不支持其他类型
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", " + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                // 该Broker是SLAVE，需要清除提交偏移量的标识
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            // 构造拉消息请求
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            // 写入消费组
            requestHeader.setConsumerGroup(this.consumerGroup);
            // 写入主题
            requestHeader.setTopic(mq.getTopic());
            // 写入队列ID
            requestHeader.setQueueId(mq.getQueueId());
            // 写入队列偏移量
            requestHeader.setQueueOffset(offset);
            // 写入最大消息数
            requestHeader.setMaxMsgNums(maxNums);
            // 写入系统标识
            requestHeader.setSysFlag(sysFlagInner);
            // 写入提交的偏移量
            requestHeader.setCommitOffset(commitOffset);
            // 写入暂停超时时间
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            // 写入订阅
            requestHeader.setSubscription(subExpression);
            // 写入子版本
            requestHeader.setSubVersion(subVersion);
            // 写入消息最大字节数
            requestHeader.setMaxMsgBytes(maxSizeInBytes);
            // 写入表达式
            requestHeader.setExpressionType(expressionType);
            // 写入Broker名称
            requestHeader.setBrokerName(mq.getBrokerName());

            String brokerAddr = findBrokerResult.getBrokerAddr();
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                // 根据类过滤器选择broker地址
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            // 向指定broker请求拉取消息，并返回结果
            return this.mQClientFactory.getMQClientAPIImpl().pullMessage(brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);
        }

        // 尽管从Namesrv获取了Broker更新，但是本地仍然没有
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public PullResult pullKernelImpl(MessageQueue mq, final String subExpression, final String expressionType, final long subVersion, long offset, final int maxNums, final int sysFlag,
        long commitOffset, final long brokerSuspendMaxTimeMillis, final long timeoutMillis, final CommunicationMode communicationMode, PullCallback pullCallback) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 拉取指定主题下，指定队列下的固定数量的消息
        return pullKernelImpl(mq, subExpression, expressionType, subVersion, offset, maxNums, Integer.MAX_VALUE, sysFlag, commitOffset, brokerSuspendMaxTimeMillis, timeoutMillis, communicationMode, pullCallback);
    }

    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr) throws MQClientException {
        // 读取内存中的主题路由信息
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            // 获取主题的路由信息
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            // 获取该主题地址下的过滤服务列表
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                // 随机选择一个服务列表
                return list.get(randomNum() % list.size());
            }
        }

        // 无法过滤，抛出异常
        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: " + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }


    /**
     *
     * @param mq
     * @param invisibleTime
     * @param maxNums
     * @param consumerGroup
     * @param timeout
     * @param popCallback
     * @param poll
     * @param initMode
    //     * @param expressionType
    //     * @param expression
     * @param order
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void popAsync(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup,
                         long timeout, PopCallback popCallback, boolean poll, int initMode, boolean order, String expressionType, String expression)
        throws MQClientException, RemotingException, InterruptedException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        }
        if (findBrokerResult != null) {
            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setInitMode(initMode);
            requestHeader.setExpType(expressionType);
            requestHeader.setExp(expression);
            requestHeader.setOrder(order);
            requestHeader.setBrokerName(mq.getBrokerName());
            //give 1000 ms for server response
            if (poll) {
                requestHeader.setPollTime(timeout);
                requestHeader.setBornTime(System.currentTimeMillis());
                // timeout + 10s, fix the too earlier timeout of client when long polling.
                timeout += 10 * 1000;
            }
            String brokerAddr = findBrokerResult.getBrokerAddr();
            this.mQClientFactory.getMQClientAPIImpl().popMessageAsync(mq.getBrokerName(), brokerAddr, requestHeader, timeout, popCallback);
            return;
        }
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

}

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.OffsetNotFoundException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

/**
 * 基于Broker的{@link OffsetStore}实现
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static Logger log = LoggerFactory.getLogger(RemoteBrokerOffsetStore.class);

    private final MQClientInstance mQClientFactory;
    /**
     * 分组名称，取{@link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#consumerGroup}
     */
    private final String groupName;

    /**
     * 保存了消费者对于消费队列的消费进度
     */
    private ConcurrentMap<MessageQueue/* 消息队列 */, ControllableOffset/* 队列偏移量 */> offsetTable = new ConcurrentHashMap<>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    @Override
    public void load() {
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            ControllableOffset offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new ControllableOffset(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    offsetOld.update(offset, true);
                } else {
                    offsetOld.update(offset);
                }
            }
        }
    }

    @Override
    public void updateAndFreezeOffset(MessageQueue mq, long offset) {
        if (mq != null) {
            this.offsetTable.computeIfAbsent(mq, k -> new ControllableOffset(offset))
                .updateAndFreeze(offset);
        }
    }

    /**
     * 从Broker读取消费者指定队列的消费偏移量
     *
     * @param mq
     * @param type
     * @return
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    // 处理内存优先的场景
                    // 获取客户端内存中的偏移量
                    ControllableOffset offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.getOffset();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        // 客户端没有，且仅从客户端读取时，返回0-1
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    // 处理内存中不存在，再次从存储中读取、以及直接从存储中读取的场景
                    try {
                        // 读取该消息队列的消费偏移量
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        // 更新内存中的消费偏移量
                        this.updateOffset(mq, brokerOffset, false);
                        // 返回偏移量
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (OffsetNotFoundException e) {
                        // 客户端还未消费过，返回-1
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        // 出现异常，返回-2
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -3;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        final HashSet<MessageQueue> unusedMQ = new HashSet<>();

        for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ControllableOffset offset = entry.getValue();
            if (offset != null) {
                if (mqs.contains(mq)) {
                    try {
                        this.updateConsumeOffsetToBroker(mq, offset.getOffset());
                        log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                            this.groupName,
                            this.mQClientFactory.getClientId(),
                            mq,
                            offset.getOffset());
                    } catch (Exception e) {
                        log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                    }
                } else {
                    unusedMQ.add(mq);
                }
            }
        }

        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
        ControllableOffset offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.getOffset());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}", this.groupName, this.mQClientFactory.getClientId(), mq, offset.getOffset());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<>(this.offsetTable.size(), 1);
        for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().getOffset());
        }
        return cloneOffsetTable;
    }

    /**
     * Update the Consumer Offset in one way, once the Master is off, updated to Slave, here need to be optimized.
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * Update the Consumer Offset synchronously, once the Master is off, updated to Slave, here need to be optimized.
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, false);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, false);
        }

        if (findBrokerResult != null) {
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);
            requestHeader.setBrokerName(mq.getBrokerName());

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        // 从内存中查找消息队列所属brokerName的broker地址
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (null == findBrokerResult) {
            // 从Namesrv上获取主题订阅信息，并更新路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            // 重新尝试查找
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, false);
        }

        if (findBrokerResult != null) {
            // 找到broker地址后，构造查询特定分组、特定主题、特定Broker集群的消费偏移量
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setBrokerName(mq.getBrokerName());

            // 发送RPC请求，
            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}

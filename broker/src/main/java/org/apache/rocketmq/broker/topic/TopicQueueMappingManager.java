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
package org.apache.rocketmq.broker.topic;

import com.alibaba.fastjson.JSON;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.TopicQueueMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;
import org.apache.rocketmq.remoting.rpc.TopicRequestHeader;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

/**
 * 用于管理主题和队列映射，主题队列映射文件路径为ENV(user.home)/store/config/topicQueueMapping.json
 */
public class TopicQueueMappingManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private transient final Lock lock = new ReentrantLock();

    //this data version should be equal to the TopicConfigManager
    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    /**
     * Map<主题, 主题队列详细信息>
     */
    private final ConcurrentMap<String/*主题*/, TopicQueueMappingDetail/*主题队列映射详情*/> topicQueueMappingTable = new ConcurrentHashMap<>();


    public TopicQueueMappingManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void updateTopicQueueMapping(TopicQueueMappingDetail newDetail, boolean force, boolean isClean, boolean flush) throws Exception {
        boolean locked = false;
        boolean updated = false;
        TopicQueueMappingDetail oldDetail = null;
        try {

            if (lock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                locked = true;
            } else {
                return;
            }
            if (newDetail == null) {
                return;
            }
            assert newDetail.getBname().equals(this.brokerController.getBrokerConfig().getBrokerName());

            newDetail.getHostedQueues().forEach((queueId, items) -> {
                TopicQueueMappingUtils.checkLogicQueueMappingItemOffset(items);
            });

            oldDetail = topicQueueMappingTable.get(newDetail.getTopic());
            if (oldDetail == null) {
                topicQueueMappingTable.put(newDetail.getTopic(), newDetail);
                updated = true;
                return;
            }
            if (force) {
                //bakeup the old items
                oldDetail.getHostedQueues().forEach((queueId, items) -> {
                    newDetail.getHostedQueues().putIfAbsent(queueId, items);
                });
                topicQueueMappingTable.put(newDetail.getTopic(), newDetail);
                updated = true;
                return;
            }
            //do more check
            if (newDetail.getEpoch() < oldDetail.getEpoch()) {
                throw new RuntimeException(String.format("Can't accept data with small epoch %d < %d", newDetail.getEpoch(), oldDetail.getEpoch()));
            }
            if (!newDetail.getScope().equals(oldDetail.getScope())) {
                throw new RuntimeException(String.format("Can't accept data with unmatched scope %s != %s", newDetail.getScope(), oldDetail.getScope()));
            }
            boolean epochEqual = newDetail.getEpoch() == oldDetail.getEpoch();
            for (Integer globalId : oldDetail.getHostedQueues().keySet()) {
                List<LogicQueueMappingItem> oldItems = oldDetail.getHostedQueues().get(globalId);
                List<LogicQueueMappingItem> newItems = newDetail.getHostedQueues().get(globalId);
                if (newItems == null) {
                    if (epochEqual) {
                        throw new RuntimeException("Cannot accept equal epoch with null data");
                    } else {
                        newDetail.getHostedQueues().put(globalId, oldItems);
                    }
                } else {
                    TopicQueueMappingUtils.makeSureLogicQueueMappingItemImmutable(oldItems, newItems, epochEqual, isClean);
                }
            }
            topicQueueMappingTable.put(newDetail.getTopic(), newDetail);
            updated = true;
        }  finally {
            if (locked) {
                this.lock.unlock();
            }
            if (updated && flush) {
                this.dataVersion.nextVersion();
                this.persist();
                log.info("Update topic queue mapping from [{}] to [{}], force {}", oldDetail, newDetail, force);
            }
        }

    }

    public void delete(final String topic) {
        TopicQueueMappingDetail old = this.topicQueueMappingTable.remove(topic);
        if (old != null) {
            log.info("delete topic queue mapping OK, static topic queue mapping: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic queue mapping failed, static topic: {} not exists", topic);
        }
    }

    public TopicQueueMappingDetail getTopicQueueMapping(String topic) {
        return topicQueueMappingTable.get(topic);
    }

    @Override
    public String encode(boolean pretty) {
        TopicQueueMappingSerializeWrapper wrapper = new TopicQueueMappingSerializeWrapper();
        wrapper.setTopicQueueMappingInfoMap(topicQueueMappingTable);
        wrapper.setDataVersion(this.dataVersion);
        return JSON.toJSONString(wrapper, pretty);
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicQueueMappingPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicQueueMappingSerializeWrapper wrapper = TopicQueueMappingSerializeWrapper.fromJson(jsonString, TopicQueueMappingSerializeWrapper.class);
            if (wrapper != null) {
                this.topicQueueMappingTable.putAll(wrapper.getTopicQueueMappingInfoMap());
                this.dataVersion.assignNewOne(wrapper.getDataVersion());
            }
        }
    }

    public ConcurrentMap<String, TopicQueueMappingDetail> getTopicQueueMappingTable() {
        return topicQueueMappingTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public TopicQueueMappingContext buildTopicQueueMappingContext(TopicRequestHeader requestHeader) {
        return buildTopicQueueMappingContext(requestHeader, false);
    }

    /**
     * 将请求头转换为TopicQueueMappingContext，不会返回空
     *
     * @param requestHeader
     * @param selectOneWhenMiss
     * @return
     */
    public TopicQueueMappingContext buildTopicQueueMappingContext(TopicRequestHeader requestHeader, boolean selectOneWhenMiss) {
        /**
         * 检查lo是否为Flase，如果是的话，表示需要进一步处理
         */
        if (requestHeader.getLo() != null && Boolean.FALSE.equals(requestHeader.getLo())) {
            return new TopicQueueMappingContext(requestHeader.getTopic(), null, null, null, null);
        }
        /**
         * 获取主题
         */
        String topic = requestHeader.getTopic();
        /**
         * 获取Client计算好的队列ID
         */
        Integer globalId = null;
        if (requestHeader instanceof TopicQueueRequestHeader) {
            globalId = ((TopicQueueRequestHeader) requestHeader).getQueueId();
        }

        /**
         * 获取该主题对应的队列映射信息
         */
        TopicQueueMappingDetail mappingDetail = getTopicQueueMapping(topic);
        if (mappingDetail == null) {
            //it is not static topic
            return new TopicQueueMappingContext(topic, null, null, null, null);
        }
        /**
         * 检查Broker名称
         */
        assert mappingDetail.getBname().equals(this.brokerController.getBrokerConfig().getBrokerName());

        /**
         * 对于没有队列ID的，直接返回
         */
        if (globalId == null) {
            return new TopicQueueMappingContext(topic, null, mappingDetail, null, null);
        }

        /**
         * 对于队列ID<0，并且不允许选择的，直接返回
         */
        if (globalId < 0 && !selectOneWhenMiss) {
            return new TopicQueueMappingContext(topic, globalId, mappingDetail, null, null);
        }

        /**
         * 对于队列ID<0进行修正
         */
        if (globalId < 0) {
            try {
                if (!mappingDetail.getHostedQueues().isEmpty()) {
                    globalId = mappingDetail.getHostedQueues().keySet().iterator().next();
                }
            } catch (Throwable ignored) {
            }
        }
        /**
         * 如果队列ID仍然<0，直接返回
         */
        if (globalId < 0) {
            return new TopicQueueMappingContext(topic, globalId,  mappingDetail, null, null);
        }

        /**
         * 获取该队列对应的逻辑文件映射信息
         */
        List<LogicQueueMappingItem> mappingItemList = TopicQueueMappingDetail.getMappingInfo(mappingDetail, globalId);
        LogicQueueMappingItem leaderItem = null;
        /**
         * 如果信息不为空，则获取最后一个
         */
        if (mappingItemList != null && mappingItemList.size() > 0) {
            leaderItem = mappingItemList.get(mappingItemList.size() - 1);
        }
        /**
         * 返回带有逻辑文件映射信息的主题队列映射上下文
         */
        return new TopicQueueMappingContext(topic, globalId, mappingDetail, mappingItemList, leaderItem);
    }


    public  RemotingCommand rewriteRequestForStaticTopic(TopicQueueRequestHeader requestHeader, TopicQueueMappingContext mappingContext) {
        try {
            /**
             * 如果队列映射信息为空，则直接返回空
             */
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }

            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", requestHeader.getTopic(), requestHeader.getQueueId(), mappingDetail.getBname()));
            }
            LogicQueueMappingItem mappingItem = mappingContext.getLeaderItem();
            requestHeader.setQueueId(mappingItem.getQueueId());
            return null;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }

}

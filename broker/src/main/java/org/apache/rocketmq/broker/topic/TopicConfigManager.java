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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableMap;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.Attribute;
import org.apache.rocketmq.common.attribute.AttributeUtil;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopicConfigManager extends ConfigManager {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final int SCHEDULE_TOPIC_QUEUE_NUM = 18;

    private transient final Lock topicConfigTableLock = new ReentrantLock();
    protected ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>(1024);
    protected DataVersion dataVersion = new DataVersion();
    protected transient BrokerController brokerController;

    public TopicConfigManager() {

    }

    public TopicConfigManager(BrokerController brokerController) {
        this(brokerController, true);
    }

    public TopicConfigManager(BrokerController brokerController, boolean init) {
        this.brokerController = brokerController;
        if (init) {
            init();
        }
    }

    protected void init() {
        {
            /**
             * 创建内置系统主题SELF_TEST_TOPIC，1个读队列，1个写多列，rw权限
             */
            String topic = TopicValidator.RMQ_SYS_SELF_TEST_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 如果启动Broker时设置了autoCreateTopicEnable=true，代表创建内置系统主题TBW102，8个读队列，8个写队列，rwx权限
             */
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getDefaultTopicQueueNums());
                /**
                 * TBW102的默认权限为可读、可写、可被继承
                 */
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                putTopicConfig(topicConfig);
            }
        }
        {
            /**
             * 创建内置系统主题BenchmarkTest，1024个读队列，1024个写队列，rw权限
             */
            String topic = TopicValidator.RMQ_SYS_BENCHMARK_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 创建内置系统主题DefaultCluster，16个读队列，16个写队列
             */
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            int perm = PermName.PERM_INHERIT;
            /**
             * 如果设置了clusterTopicEnable=true，权限设置追加可读、可写
             */
            if (this.brokerController.getBrokerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 使用当前主机名创建内置系统主题，1个读队列，1个写队列
             */
            String topic = this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            int perm = PermName.PERM_INHERIT;
            /**
             * 如果设置了brokerTopicEnable=true，权限设置追加可读、可写
             */
            if (this.brokerController.getBrokerConfig().isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 创建内置系统主题OFFSET_MOVED_EVENT，1个读队列，1个写队列，rw权限
             */
            String topic = TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 创建内置系统主题SCHEDULE_TOPIC_XXXX，18个读队列，18个写队列，rw权限
             */
            String topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(SCHEDULE_TOPIC_QUEUE_NUM);
            topicConfig.setWriteQueueNums(SCHEDULE_TOPIC_QUEUE_NUM);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 如果设置了traceTopicEnable=true，则创建内置系统主题RMQ_SYS_TRACE_TOPIC，1个读队列，1个写队列，rw权限
             */
            if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
                String topic = this.brokerController.getBrokerConfig().getMsgTraceTopicName();
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                putTopicConfig(topicConfig);
            }
        }
        {
            /**
             * 创建内置系统主题DefaultCluster_REPLY_TOPIC，1个读队列，1个写队列，rw权限
             */
            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 创建内置系统主题rmq_sysREVIVE_LOG_，8个读队列，8个写队列，rw权限
             */
            String topic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(this.brokerController.getBrokerConfig().getReviveQueueNum());
            topicConfig.setWriteQueueNums(this.brokerController.getBrokerConfig().getReviveQueueNum());
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 创建内置系统主题rmq_sys_SYNC_BROKER_MEMBER+主机名，1个读队列，1个写队列，x权限
             */
            String topic = TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX + this.brokerController.getBrokerConfig().getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(PermName.PERM_INHERIT);
            putTopicConfig(topicConfig);
        }
        {
            /**
             * 创建内置系统主题RMQ_SYS_TRANS_HALF_TOPIC，1个读队列，1个写队列，rw权限
             */
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            putTopicConfig(topicConfig);
        }

        {
            /**
             * 创建内置系统主题RMQ_SYS_TRANS_OP_HALF_TOPIC，1个读队列，1个写队列，rw权限
             */
            String topic = TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            TopicValidator.addSystemTopic(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            putTopicConfig(topicConfig);
        }

        {
            /**
             * 如果设置了timerWheelEnable=true，则创建系统内置主题rmq_sys_wheel_timer，1个读队列，1个写队列，rw权限
             */
            if (this.brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
                String topic = TimerMessageStore.TIMER_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                TopicValidator.addSystemTopic(topic);
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
    }

    public TopicConfig putTopicConfig(TopicConfig topicConfig) {
        return this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
    }

    protected TopicConfig getTopicConfig(String topicName) {
        return this.topicConfigTable.get(topicName);
    }

    protected TopicConfig removeTopicConfig(String topicName) {
        return this.topicConfigTable.remove(topicName);
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return getTopicConfig(topic);
    }

    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic, final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            /**
             * 获取主题配置表的锁
             */
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    /**
                     * 获取主题配置
                     */
                    topicConfig = getTopicConfig(topic);
                    /**
                     * 配置已经被创建，则直接返回
                     */
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    /**
                     * 获取默认的主题配置
                     */
                    TopicConfig defaultTopicConfig = getTopicConfig(defaultTopic);
                    if (defaultTopicConfig != null) {
                        /**
                         * 如果默认主题是TBW102，并且未开启自动创建主题开关，则更新默认主题配置为6，即可读可写
                         */
                        if (defaultTopic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        /**
                         * 如果默认主题可被继承，则参考默认主题创建新主题
                         */
                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);

                            /**
                             * 从 8 和 4 种取较小值
                             */
                            int queueNums = Math.min(clientDefaultTopicQueueNums, defaultTopicConfig.getWriteQueueNums());

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            /**
                             * 设置权限为不可继承
                             */
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            /**
                             * 继承默认主题的单标签过滤
                             */
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            /**
                             * 输出到warn日志中
                             */
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]", defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        /**
                         * 默认配置不存在，无法创建主题
                         */
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]", defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]", defaultTopic, topicConfig, remoteAddress);

                        /**
                         * 放到{@link #topicConfigTable}
                         */
                        putTopicConfig(topicConfig);

                        /**
                         * 更新数据版本
                         */
                        updateDataVersion();

                        /**
                         * 更新创建新主题标识位
                         */
                        createNew = true;

                        /**
                         * 持久化保存
                         */
                        this.persist();
                    }
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            /**
             * 将主题信息发送到所有的Namesrv
             */
            registerBrokerData(topicConfig);
        }

        return topicConfig;
    }

    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig) {
        return createTopicIfAbsent(topicConfig, true);
    }

    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig, boolean register) {
        boolean createNew = false;
        if (topicConfig == null) {
            throw new NullPointerException("TopicConfig");
        }
        if (StringUtils.isEmpty(topicConfig.getTopicName())) {
            throw new IllegalArgumentException("TopicName");
        }

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicConfig existedTopicConfig = getTopicConfig(topicConfig.getTopicName());
                    if (existedTopicConfig != null) {
                        return existedTopicConfig;
                    }
                    log.info("Create new topic [{}] config:[{}]", topicConfig.getTopicName(), topicConfig);
                    putTopicConfig(topicConfig);
                    updateDataVersion();
                    createNew = true;
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicIfAbsent ", e);
        }
        if (createNew && register) {
            registerBrokerData(topicConfig);
        }
        return getTopicConfig(topicConfig.getTopicName());
    }

    public TopicConfig createTopicInSendMessageBackMethod(
        final String topic,
        final int clientDefaultTopicQueueNums,
        final int perm,
        final int topicSysFlag) {
        return createTopicInSendMessageBackMethod(topic, clientDefaultTopicQueueNums, perm, false, topicSysFlag);
    }

    public TopicConfig createTopicInSendMessageBackMethod(
        final String topic,
        final int clientDefaultTopicQueueNums,
        final int perm,
        final boolean isOrder,
        final int topicSysFlag) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig != null) {
            if (isOrder != topicConfig.isOrder()) {
                topicConfig.setOrder(isOrder);
                this.updateTopicConfig(topicConfig);
            }
            return topicConfig;
        }

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = getTopicConfig(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);
                    topicConfig.setOrder(isOrder);

                    log.info("create new topic {}", topicConfig);
                    putTopicConfig(topicConfig);
                    createNew = true;
                    updateDataVersion();
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            registerBrokerData(topicConfig);
        }

        return topicConfig;
    }

    public TopicConfig createTopicOfTranCheckMaxTime(final int clientDefaultTopicQueueNums, final int perm) {
        TopicConfig topicConfig = getTopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        if (topicConfig != null)
            return topicConfig;

        boolean createNew = false;

        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = getTopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    if (topicConfig != null)
                        return topicConfig;

                    topicConfig = new TopicConfig(TopicValidator.RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(0);

                    log.info("create new topic {}", topicConfig);
                    putTopicConfig(topicConfig);
                    createNew = true;
                    updateDataVersion();
                    this.persist();
                } finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", e);
        }

        if (createNew) {
            registerBrokerData(topicConfig);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            putTopicConfig(topicConfig);

            updateDataVersion();

            this.persist();
            registerBrokerData(topicConfig);
        }
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}", oldTopicSysFlag,
                topicConfig.getTopicSysFlag());

            putTopicConfig(topicConfig);

            updateDataVersion();

            this.persist();
            registerBrokerData(topicConfig);
        }
    }

    protected void updateSingleTopicConfigWithoutPersist(final TopicConfig topicConfig) {
        checkNotNull(topicConfig, "topicConfig shouldn't be null");

        Map<String, String> newAttributes = request(topicConfig);
        Map<String, String> currentAttributes = current(topicConfig.getTopicName());

        Map<String, String> finalAttributes = AttributeUtil.alterCurrentAttributes(
            this.topicConfigTable.get(topicConfig.getTopicName()) == null,
            TopicAttributes.ALL,
            ImmutableMap.copyOf(currentAttributes),
            ImmutableMap.copyOf(newAttributes));

        topicConfig.setAttributes(finalAttributes);
        updateTieredStoreTopicMetadata(topicConfig, newAttributes);

        TopicConfig old = putTopicConfig(topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        updateDataVersion();
    }

    public void updateTopicConfig(final TopicConfig topicConfig) {
        updateSingleTopicConfigWithoutPersist(topicConfig);
        this.persist(topicConfig.getTopicName(), topicConfig);
    }

    public void updateTopicConfigList(final List<TopicConfig> topicConfigList) {
        topicConfigList.forEach(this::updateSingleTopicConfigWithoutPersist);
        this.persist();
    }

    private synchronized void updateTieredStoreTopicMetadata(final TopicConfig topicConfig, Map<String, String> newAttributes) {
        if (!(brokerController.getMessageStore() instanceof TieredMessageStore)) {
            if (newAttributes.get(TopicAttributes.TOPIC_RESERVE_TIME_ATTRIBUTE.getName()) != null) {
                throw new IllegalArgumentException("Update topic reserveTime not supported");
            }
            return;
        }

        String topic = topicConfig.getTopicName();
        long reserveTime = TopicAttributes.TOPIC_RESERVE_TIME_ATTRIBUTE.getDefaultValue();
        String attr = topicConfig.getAttributes().get(TopicAttributes.TOPIC_RESERVE_TIME_ATTRIBUTE.getName());
        if (attr != null) {
            reserveTime = Long.parseLong(attr);
        }

        log.info("Update tiered storage metadata, topic {}, reserveTime {}", topic, reserveTime);
        TieredMessageStore tieredMessageStore = (TieredMessageStore) brokerController.getMessageStore();
        MetadataStore metadataStore = tieredMessageStore.getMetadataStore();
        TopicMetadata topicMetadata = metadataStore.getTopic(topic);
        if (topicMetadata == null) {
            metadataStore.addTopic(topic, reserveTime);
        } else if (topicMetadata.getReserveTime() != reserveTime) {
            topicMetadata.setReserveTime(reserveTime);
            metadataStore.updateTopic(topicMetadata);
        }
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = getTopicConfig(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            if (isChange) {
                updateDataVersion();
                this.persist();
            }
        }
    }

    // make it testable
    public Map<String, Attribute> allAttributes() {
        return TopicAttributes.ALL;
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = removeTopicConfig(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            updateDataVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        DataVersion dataVersionCopy = new DataVersion();
        dataVersionCopy.assignNewOne(this.dataVersion);
        topicConfigSerializeWrapper.setDataVersion(dataVersionCopy);
        return topicConfigSerializeWrapper;
    }

    public TopicConfigAndMappingSerializeWrapper buildSerializeWrapper(final ConcurrentMap<String, TopicConfig> topicConfigTable) {
        return buildSerializeWrapper(topicConfigTable, Maps.newHashMap());
    }

    public TopicConfigAndMappingSerializeWrapper buildSerializeWrapper(
        final ConcurrentMap<String, TopicConfig> topicConfigTable,
        final Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap
    ) {
        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        topicConfigWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        topicConfigWrapper.setDataVersion(this.getDataVersion());
        if (this.brokerController.getBrokerConfig().isEnableSplitRegistration()) {
            this.getDataVersion().nextVersion();
        }
        return topicConfigWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    public boolean loadDataVersion() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            if (jsonString != null) {
                TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                    TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
                if (topicConfigSerializeWrapper != null) {
                    this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                    log.info("load topic metadata dataVersion success {}, {}", fileName, topicConfigSerializeWrapper.getDataVersion());
                }
            }
            return true;
        } catch (Exception e) {
            log.error("load topic metadata dataVersion failed" + fileName, e);
            return false;
        }
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, TopicConfig>> it = tcs.getTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setTopicConfigTable(
        ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    private Map<String, String> request(TopicConfig topicConfig) {
        return topicConfig.getAttributes() == null ? new HashMap<>() : topicConfig.getAttributes();
    }

    private Map<String, String> current(String topic) {
        TopicConfig topicConfig = getTopicConfig(topic);
        if (topicConfig == null) {
            return new HashMap<>();
        } else {
            Map<String, String> attributes = topicConfig.getAttributes();
            if (attributes == null) {
                return new HashMap<>();
            } else {
                return attributes;
            }
        }
    }

    private void registerBrokerData(TopicConfig topicConfig) {
        /**
         * 如果设置了enableSingleTopicRegister=true，则将主题信息注册到所有的Namesrv上
         */
        if (brokerController.getBrokerConfig().isEnableSingleTopicRegister()) {
            this.brokerController.registerSingleTopicAll(topicConfig);
        } else {
            /**
             * 将增量的Broker信息注册到所有的Namesrv上
             */
            this.brokerController.registerIncrementBrokerData(topicConfig, dataVersion);
        }
    }

    public boolean containsTopic(String topic) {
        return topicConfigTable.containsKey(topic);
    }

    public void updateDataVersion() {
        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);
    }



}

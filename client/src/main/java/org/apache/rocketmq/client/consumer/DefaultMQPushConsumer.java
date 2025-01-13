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
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 消费者消费消息入口类，这也是最常推荐使用的。
 * <p>
 * Push客户端底层基于Pull服务，当消息从Broker拉取之后，通过调用注册的监听器回调来消费消息。
 * <p>
 * 线程安全类
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

    private final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumer.class);

    /**
     * Internal implementation. Most of the functions herein are delegated to it.
     */
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * 消费者组，相同订阅和相同消费者组内的消费者实现负载均衡
     */
    private String consumerGroup;

    /**
     * 消息模式，默认为集群模式，即相同订阅和消费者组内的消费者瓜分消息；如果设置为广播模式，则每个消费者都消费一样的消息
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * 消费引导时的时间点，默认为从上次消费停止的地方继续消费
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * 消费时间戳，当前时间往前30分钟，即在选择{@link ConsumeFromWhere#CONSUME_FROM_TIMESTAMP}时，使用该时间戳作为消费时间点。
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * 队列分配算法，指示消息队列如何分配给每个消费者客户端
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * 保存了用户订阅时传递的主题和订阅表达式信息
     * <ul>
     *     <li>{@link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#subscribe(String, String)}</li>
     *     <li>{@link org.apache.rocketmq.client.consumer.DefaultLitePullConsumer#subscribe(String, String)}</li>
     * </ul>
     */
    private Map<String /* topic */, String /* 订阅表达式 */> subscription = new HashMap<>();

    /**
     * 消费者监听器
     */
    private MessageListener messageListener;

    /**
     * 消息队列监听器，当消息队列分配发生变化时，调用该监听器
     */
    private MessageQueueListener messageQueueListener;

    /**
     * 队列偏移量存储
     */
    private OffsetStore offsetStore;

    /**
     * 最小的消费者线程数量，默认为20，有效值为[1, 1000]
     */
    private int consumeThreadMin = 20;

    /**
     * 最大的消费者线程数量，默认为20，有效值为[1, 1000]
     */
    private int consumeThreadMax = 20;

    /**
     * 动态调整线程池数量的阈值，默认为10w
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * 并发消费时最大跨度偏移量，在顺序消费时没有影响，默认为2000
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * 拉队列阈值。默认为1000。消息队列级别的流控，每个消息队列默认缓存最多1000条消息。考虑{@link #pullBatchSize}，瞬时值可能超过限制。有效范围[1, 65535]
     */
    private int pullThresholdForQueue = 1000;

    /**
     * 最大等待ACK的消息数量，默认为96.消息队列级别的流控，与{@link #pullThresholdForQueue}相反，一旦消息被弹出，就被认为是消费的开始
     */
    private int popThresholdForQueue = 96;

    /**
     * 限制队列级别缓存的消息大小，每个消息队列默认缓存至多100M，考虑到{@link #pullBatchSize}，瞬时值可能超过限制。
     * <p>
     * 消息大小仅通过消息体来衡量，因此不准确
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * Flow control threshold on topic level, default value is -1(Unlimited)
     * <p>
     * The value of {@code pullThresholdForQueue} will be overwritten and calculated based on
     * {@code pullThresholdForTopic} if it isn't unlimited
     * <p>
     * For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
     * then pullThresholdForQueue will be set to 100
     *
     * 拉队列阈值。默认为-1，即不限制。主题队列级别的流控，当设置了该值且不为-1，将覆盖{@link #pullThresholdForQueue}。有效范围[1, 6553500]
     * <p>
     * 例如将该值设置为1000，主题下有10个消息队列被分配到该消费者，那么{@link #pullThresholdForQueue}=100
     */
    private int pullThresholdForTopic = -1;

    /**
     * 限制主题级别缓存的消息大小，默认为-1（不限制）。当设置了该值且不为-1时，将覆盖{@link #pullThresholdSizeForQueue}。有效范围[1, 102400]，单位为MB。
     * <p>
     * 例如将该值设置为1000，主题下有10个消息队列被分配到该消息者，那么{@link #pullThresholdSizeForQueue}=100
     */
    private int pullThresholdSizeForTopic = -1;

    /**
     * 消息拉取间隔，默认为0
     */
    private long pullInterval = 0;

    /**
     * 批次消息的最大消息数，默认为1，即{@link MessageListenerConcurrently#consumeMessage(List, ConsumeConcurrentlyContext)}，第一个参数返回的消息数目为1。有效范围[1, 1024]
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * 批次拉取的消息数，默认为32。
     */
    private int pullBatchSize = 32;

    private int pullBatchSizeInBytes = 256 * 1024;

    /**
     * Whether update subscription relationship when every pull
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    /**
     * Max re-consume times.
     * In concurrently mode, -1 means 16;
     * In orderly mode, -1 means Integer.MAX_VALUE.
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success.
     */
    private int maxReconsumeTimes = -1;

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * 消息可能阻塞使用线程的最大时间，单位为minute，默认为15m
     */
    private long consumeTimeout = 15;

    /**
     * 消息最大不可见时间，单位为毫秒，默认为60s，有效范围为[5000, 300000]
     */
    private long popInvisibleTime = 60000;

    /**
     * 批次弹出大小，默认为32，有效范围为[1, 32]
     */
    private int popBatchNums = 32;

    /**
     * Maximum time to await message consuming when shutdown consumer, 0 indicates no await.
     */
    private long awaitTerminationMillisWhenShutdown = 0;

    /**
     * Interface of asynchronous transfer data
     */
    private TraceDispatcher traceDispatcher = null;

    // force to use client rebalance
    private boolean clientRebalance = true;

    private RPCHook rpcHook = null;

    /**
     * Default constructor.
     */
    public DefaultMQPushConsumer() {
        this(MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String consumerGroup) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group, RPC hook.
     *
     * @param consumerGroup Consumer group.
     * @param rpcHook       RPC hook to execute before each remoting command.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook) {
        this(consumerGroup, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup        Consumer group.
     * @param enableMsgTrace       Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param consumerGroup                Consumer group.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy Message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this(consumerGroup, rpcHook, allocateMessageQueueStrategy, false, null);
    }

    /**
     * Constructor specifying consumer group, RPC hook, message queue allocating algorithm, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup                Consumer group.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     * @param enableMsgTrace               Switch flag instance for message trace.
     * @param customizedTraceTopic         The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this.consumerGroup = consumerGroup;
        this.rpcHook = rpcHook;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
        this.enableTrace = enableMsgTrace;
        this.traceTopic = customizedTraceTopic;
    }

    /**
     * Constructor specifying namespace and consumer group.
     *
     * @param namespace     Namespace for this MQ Producer instance.
     * @param consumerGroup Consumer group.
     */
    @Deprecated
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup) {
        this(namespace, consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook .
     *
     * @param namespace     Namespace for this MQ Producer instance.
     * @param consumerGroup Consumer group.
     * @param rpcHook       RPC hook to execute before each remoting command.
     */
    @Deprecated
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this(namespace, consumerGroup, rpcHook, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying namespace, consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param namespace                    Namespace for this MQ Producer instance.
     * @param consumerGroup                Consumer group.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy Message queue allocating algorithm.
     */
    @Deprecated
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.rpcHook = rpcHook;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying namespace, consumer group, RPC hook, message queue allocating algorithm, enabled msg trace flag and customized trace topic name.
     *
     * @param namespace                    Namespace for this MQ Producer instance.
     * @param consumerGroup                Consumer group.
     * @param rpcHook                      RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     * @param enableMsgTrace               Switch flag instance for message trace.
     * @param customizedTraceTopic         The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    @Deprecated
    public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace,
        final String customizedTraceTopic) {
        this.consumerGroup = consumerGroup;
        this.namespace = namespace;
        this.rpcHook = rpcHook;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
        this.enableTrace = enableMsgTrace;
        this.traceTopic = customizedTraceTopic;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum,
        Map<String, String> attributes) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0, null);
    }

    @Override
    public void setUseTLS(boolean useTLS) {
        super.setUseTLS(useTLS);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag,
        Map<String, String> attributes) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(msgId);
            return this.defaultMQPushConsumerImpl.viewMessage(withNamespace(topic), msgId);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPushConsumerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPopThresholdForQueue() {
        return popThresholdForQueue;
    }

    public void setPopThresholdForQueue(int popThresholdForQueue) {
        this.popThresholdForQueue = popThresholdForQueue;
    }

    public int getPullThresholdForTopic() {
        return pullThresholdForTopic;
    }

    public void setPullThresholdForTopic(final int pullThresholdForTopic) {
        this.pullThresholdForTopic = pullThresholdForTopic;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(final int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getPullThresholdSizeForTopic() {
        return pullThresholdSizeForTopic;
    }

    public void setPullThresholdSizeForTopic(final int pullThresholdSizeForTopic) {
        this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
    }

    public Map<String, String> getSubscription() {
        return subscription;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setSubscription(Map<String, String> subscription) {
        Map<String, String> subscriptionWithNamespace = new HashMap<>(subscription.size(), 1);
        for (Entry<String, String> topicEntry : subscription.entrySet()) {
            subscriptionWithNamespace.put(withNamespace(topicEntry.getKey()), topicEntry.getValue());
        }
        this.subscription = subscriptionWithNamespace;
    }

    /**
     * Send message back to broker which will be re-delivered in future.
     * <p>
     * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
     * please do not use this method.
     *
     * @param msg        Message to send back.
     * @param delayLevel delay level.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException    if there is any client error.
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, msg.getBrokerName());
    }

    /**
     * Send message back to the broker whose name is <code>brokerName</code> and the message will be re-delivered in
     * future.
     * <p>
     * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
     * please do not use this method.
     *
     * @param msg        Message to send back.
     * @param delayLevel delay level.
     * @param brokerName broker name.
     * @throws RemotingException    if there is any network-tier error.
     * @throws MQBrokerException    if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException    if there is any client error.
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }

    /**
     * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
     *
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void start() throws MQClientException {
        // 设置消费者组，格式为[%RETRY%|%DLQ%]namespace%consumerGroup
        setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        // 启动内部MQ消费者
        this.defaultMQPushConsumerImpl.start();
        if (enableTrace) {
            try {
                // 基于消费者组、主题构造异步追踪器
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, getTraceMsgBatchNum(), traceTopic, rpcHook);
                // 设置目标消费者为PushConsumer
                dispatcher.setHostConsumer(this.defaultMQPushConsumerImpl);
                // 设置命名空间
                dispatcher.setNamespaceV2(namespaceV2);
                traceDispatcher = dispatcher;
                // 注册消费消息回调
                this.defaultMQPushConsumerImpl.registerConsumeMessageHook(new ConsumeMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
        // 如果设置了开启消息追踪，则启动该追踪器
        if (null != traceDispatcher) {
            if (traceDispatcher instanceof AsyncTraceDispatcher) {
                ((AsyncTraceDispatcher) traceDispatcher).getTraceProducer().setUseTLS(isUseTLS());
            }
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * Shut down this client and releasing underlying resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown(awaitTerminationMillisWhenShutdown);
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    @Override
    @Deprecated
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * 注册一个回调函数，在消息到达时执行，以实现并发消费
     *
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        /**
         * 注册到底层实现上
         */
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for orderly consuming.
     *
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Subscribe a topic to consuming subscription.
     *
     * @param topic         topic to subscribe.
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     *                      if null or * expression,meaning subscribe all
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), subExpression);
    }

    /**
     * Subscribe a topic to consuming subscription.
     *
     * @param topic             topic to consume.
     * @param fullClassName     full class name,must extend org.apache.rocketmq.common.filter. MessageFilter
     * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety
     */
    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), fullClassName, filterClassSource);
    }

    /**
     * Subscribe a topic by message selector.
     *
     * @param topic           topic to consume.
     * @param messageSelector {@link org.apache.rocketmq.client.consumer.MessageSelector}
     * @see org.apache.rocketmq.client.consumer.MessageSelector#bySql
     * @see org.apache.rocketmq.client.consumer.MessageSelector#byTag
     */
    @Override
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), messageSelector);
    }

    /**
     * Un-subscribe the specified topic from subscription.
     *
     * @param topic message topic
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }

    /**
     * Update the message consuming thread core pool size.
     *
     * @param corePoolSize new core pool size.
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }

    /**
     * Suspend pulling new messages.
     */
    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }

    /**
     * Resume pulling.
     */
    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }

    public boolean isPause() {
        return this.defaultMQPushConsumerImpl.isPause();
    }

    public boolean isConsumeOrderly() {
        return this.defaultMQPushConsumerImpl.isConsumeOrderly();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.defaultMQPushConsumerImpl.registerConsumeMessageHook(hook);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }

    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }

    @Override
    public boolean isUnitMode() {
        return unitMode;
    }

    @Override
    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }

    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(final long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(final long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public long getPopInvisibleTime() {
        return popInvisibleTime;
    }

    public void setPopInvisibleTime(long popInvisibleTime) {
        this.popInvisibleTime = popInvisibleTime;
    }

    public long getAwaitTerminationMillisWhenShutdown() {
        return awaitTerminationMillisWhenShutdown;
    }

    public void setAwaitTerminationMillisWhenShutdown(long awaitTerminationMillisWhenShutdown) {
        this.awaitTerminationMillisWhenShutdown = awaitTerminationMillisWhenShutdown;
    }

    public int getPullBatchSizeInBytes() {
        return pullBatchSizeInBytes;
    }

    public void setPullBatchSizeInBytes(int pullBatchSizeInBytes) {
        this.pullBatchSizeInBytes = pullBatchSizeInBytes;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

    public int getPopBatchNums() {
        return popBatchNums;
    }

    public void setPopBatchNums(int popBatchNums) {
        this.popBatchNums = popBatchNums;
    }

    public boolean isClientRebalance() {
        return clientRebalance;
    }

    public void setClientRebalance(boolean clientRebalance) {
        this.clientRebalance = clientRebalance;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }
}

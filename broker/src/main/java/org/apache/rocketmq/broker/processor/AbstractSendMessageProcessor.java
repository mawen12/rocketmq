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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.DBMsgConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected static final Logger DLQ_LOG = LoggerFactory.getLogger(LoggerName.DLQ_LOGGER_NAME);

    protected List<ConsumeMessageHook> consumeMessageHookList;

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    private List<SendMessageHook> sendMessageHookList;

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    protected RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        // 反序列化请求头
        final ConsumerSendMsgBackRequestHeader requestHeader = request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

        // 发送回的请求如果到达了Slave，则会被转发到Master
        final BrokerController masterBroker = this.brokerController.peekMasterBroker();
        if (null == masterBroker) {
            // 在集群内没有找到Master节点，应该报错，因为Slave本身无法执行写操作
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("no master available along with " + brokerController.getBrokerConfig().getBrokerIP1());
            return response;
        }

        // 接受请求的主机，可能是MASTER或者SLAVE
        final BrokerController currentBroker = this.brokerController;
        // 查询内存中的订阅分组配置，如果不存在，则进行自动创建，如果无法创建，返回null
        SubscriptionGroupConfig subscriptionGroupConfig = masterBroker.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            // 配置不存在，抛出配置分组不存在的异常
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        // 从MASTER上获取Broker配置
        BrokerConfig masterBrokerConfig = masterBroker.getBrokerConfig();
        if (!PermName.isWriteable(masterBrokerConfig.getBrokerPermission())) {
            // 该MASTER没有写权限，抛出没有权限异常
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + masterBrokerConfig.getBrokerIP1() + "] sending message is forbidden");
            return response;
        }

        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            // 该分组的重试队列队列<=0，代表没有重试次数了，直接返回成功
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        // 使用当前分组构造一个新主题，%RETRY%consumerGroup
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        // 根据重试队列数量随机选择一个重试队列ID
        int queueIdInt = this.random.nextInt(subscriptionGroupConfig.getRetryQueueNums());

        // 主题系统标识
        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        // 创建重试主题的配置
        TopicConfig topicConfig = masterBroker.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.getRetryQueueNums(), PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            // 主题无法创建，例如锁超时，返回系统错误
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return response;
        }

        if (!PermName.isWriteable(topicConfig.getPerm())) {
            // 该主题没有写权限，抛出没有权限异常
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return response;
        }

        // 从原存储中读取该偏移量的消息
        MessageExt msgExt = currentBroker.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            // 原存储中消息不存在，可能已经被删除了，返回系统错误
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return response;
        }

        // 获取消息属性RETRY_TOPIC，即重试主题
        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            // 未指定，则采用当前主题作为重试主题
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        // 消息无序等待存储OK
        msgExt.setWaitStoreMsgOK(false);
        // 读取消息延迟级别
        int delayLevel = requestHeader.getDelayLevel();

        // 读取订阅分组配置的最大重试次数
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            // 客户端版本>=3.9，从客户端请求头读取最大消费次数
            Integer times = requestHeader.getMaxReconsumeTimes();
            if (times != null) {
                // 订阅分组配置中未提供最大消费次数，使用请求头传递的
                maxReconsumeTimes = times;
            }
        }

        // 默认非延迟消息
        boolean isDLQ = false;
        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) {
            // 当重新消费次数达到最大消费次数，或者没有指定延迟登记，该消息被视作延迟消息
            // 以消费分组、原始主题、是否系统主题构造属性
            Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                    .put(LABEL_CONSUMER_GROUP, requestHeader.getGroup())
                    .put(LABEL_TOPIC, requestHeader.getOriginTopic())
                    .put(LABEL_IS_SYSTEM, BrokerMetricsManager.isSystem(requestHeader.getOriginTopic(), requestHeader.getGroup()))
                    .build();
            // 增加发送延迟队列消息的指标
            BrokerMetricsManager.sendToDlqMessages.add(1, attributes);
            // 该消息被视作延迟消息
            isDLQ = true;
            // 使用当前分组构造一个新主题，%DLQ%consumerGroup
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            // 默认取第0个队列
            queueIdInt = randomQueueId(DLQ_NUMS_PER_GROUP);

            // 在MASTER上创建DLQ主题
            topicConfig = masterBroker.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE | PermName.PERM_READ, 0);

            if (null == topicConfig) {
                // 创建结果为空，返回系统错误异常
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }
            // 设置延迟时间登记为0
            msgExt.setDelayTimeLevel(0);
        } else {
            // 尚未达到重试上限且延迟级别>=0
            if (0 == delayLevel) {
                // 如果延迟级别为0，则更新为3+重新消费次数
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            // 更新延迟级别
            msgExt.setDelayTimeLevel(delayLevel);
        }

        // 构造消息
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        // 写入主题
        msgInner.setTopic(newTopic);
        // 写入消息体
        msgInner.setBody(msgExt.getBody());
        // 写入消息标识
        msgInner.setFlag(msgExt.getFlag());
        // 写入消息属性
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        // 写入消息属性字符串
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        // 写入标签代码
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        // 写入队列ID
        msgInner.setQueueId(queueIdInt);
        // 写入系统标识
        msgInner.setSysFlag(msgExt.getSysFlag());
        // 写入消息的存储时间
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        // 写入消息的发送主机
        msgInner.setBornHost(msgExt.getBornHost());
        // 写入消息的存储主机
        msgInner.setStoreHost(this.getStoreHost());
        // 更新消息最大重试次数+1
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);
        // 获取消息ID
        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        // 写入消息ID
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
        // TODO by mawen，之前代码已经设置过了
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        boolean succeeded = false;

        // 将重试主题存储到MASTER的消息存储中
        PutMessageResult putMessageResult = masterBroker.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            String commercialOwner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);

            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(msgInner.getTopic())) {
                        masterBroker.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                        masterBroker.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                        masterBroker.getBrokerStatsManager().incQueuePutNums(msgInner.getTopic(), msgInner.getQueueId());
                        masterBroker.getBrokerStatsManager().incQueuePutSize(msgInner.getTopic(), msgInner.getQueueId(), putMessageResult.getAppendMessageResult().getWroteBytes());
                    }
                    masterBroker.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

                    if (isDLQ) {
                        masterBroker.getBrokerStatsManager().incDLQStatValue(
                                BrokerStatsManager.SNDBCK2DLQ_TIMES,
                                commercialOwner,
                                requestHeader.getGroup(),
                                requestHeader.getOriginTopic(),
                                BrokerStatsManager.StatsType.SEND_BACK_TO_DLQ.name(),
                                1);

                        String uniqKey = msgInner.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                        DLQ_LOG.info("send msg to DLQ {}, owner={}, originalTopic={}, consumerId={}, msgUniqKey={}, storeTimestamp={}",
                                newTopic,
                                commercialOwner,
                                requestHeader.getOriginTopic(),
                                requestHeader.getGroup(),
                                uniqKey,
                                putMessageResult.getAppendMessageResult().getStoreTimestamp());
                    }

                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);

                    succeeded = true;
                    break;
                default:
                    break;
            }

            if (!succeeded) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(putMessageResult.getPutMessageStatus().name());
            }
        } else {
            if (isDLQ) {
                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
                String uniqKey = msgInner.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                DLQ_LOG.info("failed to send msg to DLQ {}, owner={}, originalTopic={}, consumerId={}, msgUniqKey={}, result={}",
                        newTopic,
                        owner,
                        requestHeader.getOriginTopic(),
                        requestHeader.getGroup(),
                        uniqKey,
                        "null");
            }

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
        }

        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setNamespace(namespace);
            context.setTopic(requestHeader.getOriginTopic());
            context.setConsumerGroup(requestHeader.getGroup());
            context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
            context.setCommercialRcvTimes(1);
            context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));

            context.setAccountAuthType(request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE));
            context.setAccountOwnerParent(request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT));
            context.setAccountOwnerSelf(request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF));
            context.setRcvStat(isDLQ ? BrokerStatsManager.StatsType.SEND_BACK_TO_DLQ : BrokerStatsManager.StatsType.SEND_BACK);
            context.setSuccess(succeeded);
            context.setRcvMsgNum(1);
            //Set msg body size 0 when sent back by consumer.
            context.setRcvMsgSize(0);
            context.setCommercialRcvMsgNum(succeeded ? 1 : 0);

            try {
                this.executeConsumeMessageHookAfter(context);
            } catch (AbortProcessException e) {
                response.setCode(e.getResponseCode());
                response.setRemark(e.getErrorMessage());
            }
        }

        return response;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * 解析请求头，并构造{@link SendMessageContext}
     *
     * @param ctx
     * @param requestHeader
     * @param request
     * @return
     */
    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader, RemotingCommand request) {
        /**
         * 获取主题上的命令空间
         */
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic());

        SendMessageContext sendMessageContext;
        sendMessageContext = new SendMessageContext();
        /**
         * 写入命名空间
         */
        sendMessageContext.setNamespace(namespace);
        /**
         * 写入生产者组
         */
        sendMessageContext.setProducerGroup(requestHeader.getProducerGroup());
        /**
         * 写入主题
         */
        sendMessageContext.setTopic(requestHeader.getTopic());
        /**
         * 写入压缩后的消息体长度
         */
        sendMessageContext.setBodyLength(request.getBody().length);
        /**
         * 写入消息属性
         */
        sendMessageContext.setMsgProps(requestHeader.getProperties());
        /**
         * 写入发送消息的主机
         */
        sendMessageContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        /**
         * 写入当前节点的主机
         */
        sendMessageContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        /**
         * 写入队列ID
         */
        sendMessageContext.setQueueId(requestHeader.getQueueId());
        /**
         * 写入Broker区域ID，默认为DefaultRegion
         */
        sendMessageContext.setBrokerRegionId(this.brokerController.getBrokerConfig().getRegionId());
        /**
         * 设置请求头创建时间
         */
        sendMessageContext.setBornTimeStamp(requestHeader.getBornTimestamp());
        /**
         * 设置请求处理时间
         */
        sendMessageContext.setRequestTimeStamp(System.currentTimeMillis());

        /**
         * 读取请求扩展Owner
         */
        sendMessageContext.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));

        /**
         * 读取请求头属性
         */
        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        /**
         * 写入MSG_REGION，默认为DefaultRegion
         */
        properties.put(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        /**
         * 写入TRACE_ON，默认为true
         */
        properties.put(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));
        /**
         * 会写请求属性
         */
        requestHeader.setProperties(MessageDecoder.messageProperties2String(properties));

        /**
         * 写入属性UNIQ_KEY，即消息ID
         */
        sendMessageContext.setMsgUniqueKey(Optional.ofNullable(properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)).orElse(""));

        /**
         * 读取请求属性__SHARDINGKEY，如果存在，则表示为顺序消息；否则为普通消息
         */
        if (properties.containsKey(MessageConst.PROPERTY_SHARDING_KEY)) {
            sendMessageContext.setMsgType(MessageType.Order_Msg);
        } else {
            sendMessageContext.setMsgType(MessageType.Normal_Msg);
        }
        return sendMessageContext;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }

    protected MessageExtBrokerInner buildInnerMsg(final ChannelHandlerContext ctx,
                                                  final SendMessageRequestHeader requestHeader, final byte[] body, TopicConfig topicConfig) {
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }
        int sysFlag = requestHeader.getSysFlag();

        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
                MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
                msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader
                .getReconsumeTimes());
        return msgInner;
    }

    public SocketAddress getStoreHost() {
        return brokerController.getStoreHost();
    }

    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,
                                              final SendMessageRequestHeader requestHeader, RemotingCommand request,
                                              final RemotingCommand response) {
        String topic = requestHeader.getTopic();
        if (topic.length() > Byte.MAX_VALUE) {
            LOGGER.warn("msgContentCheck: message topic length is too long, topic={}, topic length={}, threshold={}",
                    topic, topic.length(), Byte.MAX_VALUE);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            LOGGER.warn(
                    "msgContentCheck: message properties length is too long, topic={}, properties length={}, threshold={}",
                    topic, requestHeader.getProperties().length(), Short.MAX_VALUE);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (request.getBody().length > DBMsgConstants.MAX_BODY_SIZE) {
            LOGGER.warn(
                    "msgContentCheck: message body size exceeds the threshold, topic={}, body size={}, threshold={}bytes",
                    topic, request.getBody().length, DBMsgConstants.MAX_BODY_SIZE);
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

    /**
     * 校验主题权限、格式、长度，配置、校验队列ID，并按需创建主题，同步到所有的Broker上
     *
     * @param ctx
     * @param requestHeader
     * @param request
     * @param response
     * @return
     */
    protected RemotingCommand msgCheck(final ChannelHandlerContext ctx, final SendMessageRequestHeader requestHeader, final RemotingCommand request, final RemotingCommand response) {
        // 如果Broker不允许写入，并且写入的主题是顺序的，则返回没有权限的错误
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission()) && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return response;
        }

        // 检查主题的格式、长度
        TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(requestHeader.getTopic());
        if (!result.isValid()) {
            // 对于校验未通过的主题，返回非法参数的错误
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(result.getRemark());
            return response;
        }

        // 如果该主题不允许被生产者发送，则返回没有权限的错误
        if (TopicValidator.isNotAllowedSendTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("Sending message to topic[" + requestHeader.getTopic() + "] is forbidden.");
            return response;
        }

        // 获取主题配置
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            LOGGER.warn("the topic {} not exist, producer: {}", requestHeader.getTopic(), ctx.channel().remoteAddress());

            // 该主题不存在，在发送消息方法中创建主题，并同步到所有的Namesrv中
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(requestHeader.getTopic(), requestHeader.getDefaultTopic(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            if (null == topicConfig) {
                // 主题无法创建，但是对于%RETRY%主题，由服务端负责创建
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
                }
            }

            if (null == topicConfig) {
                // 主题配置不存在，返回对应错误
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!" + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        // 获取消息发送的队列ID
        int queueIdInt = requestHeader.getQueueId();

        // 判断队列ID是否在可写的队列数量范围内，如果队列ID错误，则返回非法参数的错误
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s", queueIdInt, topicConfig, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(errorInfo);

            return response;
        }

        return response;
    }

    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }

    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request,
                              final RemotingCommand response) {
        NettyRemotingAbstract.writeResponse(ctx.channel(), request, response);
    }

    public void executeSendMessageHookBefore(SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    /**
                     * 执行回调
                     */
                    hook.sendMessageBefore(context);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Throwable e) {
                    //ignore
                }
            }
        }
    }

    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        return SendMessageRequestHeader.parseRequestHeader(request);
    }

    protected int randomQueueId(int writeQueueNums) {
        return ThreadLocalRandom.current().nextInt(99999999) % writeQueueNums;
    }

    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    //ignore
                }
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}

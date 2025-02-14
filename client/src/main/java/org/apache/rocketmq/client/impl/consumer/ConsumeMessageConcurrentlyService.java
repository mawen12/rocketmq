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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.mawen.CorePart;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 基于并发消息消费的{@link ConsumeMessageService}实现。
 *
 * <p>基于Push的消费类型
 *
 * <p> 其中并发消费时通过{@link #consumeExecutor}来实现的，默认创建20个线程执行并发消费。
 *
 * @see org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType#CONSUME_PASSIVELY
 */
@CorePart(value = "并发消费消息的实现", part = CorePart.Part.CONSUMER)
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

    private static final Logger log = LoggerFactory.getLogger(ConsumeMessageConcurrentlyService.class);

    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    private final DefaultMQPushConsumer defaultMQPushConsumer;
    /**
     * 用户注册的并发消息消息监听器
     */
    private final MessageListenerConcurrently messageListener;
    /**
     * 保存消费任务的队列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**
     * 消费任务执行器，最小线程数默认20，最大线程数默认20，超时时间60s，消费队列实现为链表，线程名称前缀为ConsumeMessageThread_consumerGroup_。
     * 该线程池就是用于并发消费推送过来的消息，并同步调用监听器。
     */
    private final ThreadPoolExecutor consumeExecutor;
    /**
     * 消费者所在的消费者分组
     */
    @CorePart(value = "消费者所在的分组", part = CorePart.Part.CONSUMER)
    private final String consumerGroup;
    /**
     * 定时调度执行器，单线程，线程名称前缀为ConsumeMessageScheduledThread_consumerGroup_，
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * 定时清理消息执行器，单线程，线程名称前缀为CleanExpireMsgScheduledThread_consumerGroup_，初始执行间隔默认为15m，后续执行间隔15m
     */
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<>();

        String consumerGroupTag = (consumerGroup.length() > 100 ? consumerGroup.substring(0, 100) : consumerGroup) + "_";
        this.consumeExecutor = new ThreadPoolExecutor(this.defaultMQPushConsumer.getConsumeThreadMin(), this.defaultMQPushConsumer.getConsumeThreadMax(), 1000 * 60, TimeUnit.MILLISECONDS, this.consumeRequestQueue, new ThreadFactoryImpl("ConsumeMessageThread_" + consumerGroupTag));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_" + consumerGroupTag));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_" + consumerGroupTag));
    }

    public void start() {
        // 开启定时任务，每隔15m执行消息清理
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(() -> {
            try {
                // 清理过期消息
                cleanExpireMsg();
            } catch (Throwable e) {
                log.error("scheduleAtFixedRate cleanExpireMsg exception", e);
            }
        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0 && corePoolSize <= Short.MAX_VALUE && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        msg.setBrokerName(brokerName);
        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(UtilAll.exceptionSimpleDesc(e));

            log.warn("consumeMessageDirectly exception: {} Group: {} Msgs: {} MQ: {}",
                UtilAll.exceptionSimpleDesc(e),
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq, e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 提供了消息过来后的处理逻辑，构造消费请求，并放入到队列中执行。
     * <p>
     * 对于消息数量超过了设置的消费最大值，进行拆分处理
     * <p>
     * 对于队列已满的情况，使用定时任务在5s后重试尝试提交。
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     * @param dispatchToConsume
     */
    @Override
    public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispatchToConsume) {
        // 获取批次消息的最大消息数
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            // 消息数小于设置的最大消费数，则直接可以消费
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                // 将消息消费提交到消费任务执行器中执行
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                // 对于队列已满的情况，将在5s后再次提交
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            // 对于消息数超过了设置的最小消费数，对齐进行拆分处理，例如110>100，则被拆分为[100, 10]两个任务去执行
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }
                // 构造消费请求
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    // 提交到消费任务执行器中
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }
                    // 对于队列已满的情况，将剩余消息一并在5s后再次提交
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    @Override
    public void submitPopConsumeRequest(final List<MessageExt> msgs, final PopProcessQueue processQueue, final MessageQueue messageQueue) {
        throw new UnsupportedOperationException();
    }

    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it = this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 根据消费状态处理消息结果
     *
     * @param status
     * @param context
     * @param consumeRequest
     */
    public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {
        // 读取确定索引
        int ackIndex = context.getAckIndex();
        // 如果消费的消息为空，则无需处理
        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {
            case CONSUME_SUCCESS:
                // 消费成功，如果ackIndex超过了消息的数量上限，则重置为消息数量-1
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                // 计算消费成功的消息数
                int ok = ackIndex + 1;
                // 计算消费失败的消息数
                int failed = consumeRequest.getMsgs().size() - ok;
                // 更新消费成功的指标
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                // 更新消费失败的指标
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                // 消费重试，代表之前消费出现问题，所有消息都是失败的
                ackIndex = -1;
                // 更新消费失败的指标
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                // 处理集群类型的消息
                List<MessageExt> msgBackFailed = new ArrayList<>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    // 忽略过期被清理的消息
                    if (!consumeRequest.getProcessQueue().containsMessage(msg)) {
                        log.info("Message is not found in its process queue; skip send-back-procedure, topic={}, " + "brokerName={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getBrokerName(), msg.getQueueId(), msg.getQueueOffset());
                        continue;
                    }

                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        // 读取消息重新消费车策略，默认为0
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // 更新消息的Topic，增加namespace，格式为[%RETRY%|%DLQ%]namespace%topic
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, this.defaultMQPushConsumer.queueWithNamespace(context.getMessageQueue()));
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg, e);
        }

        return false;
    }

    private void submitConsumeRequestLater(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue) {
        // 在当前时间延后5s再次提交消费请求
        this.scheduledExecutorService.schedule(() -> ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true), 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {
        // 在当前时间延后5s再次提交消费请求
        this.scheduledExecutorService.schedule(() -> {ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);}, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 消费消息的请求
     */
    class ConsumeRequest implements Runnable {
        /**
         * 代消费的消息集
         */
        private final List<MessageExt> msgs;
        /**
         *
         */
        private final ProcessQueue processQueue;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }
            // 读取监听器
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            // 构造并发消费上下文
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.tryResetPopRetryTopic(msgs, consumerGroup);
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                // 消费者的命名空间
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                // 消费者组
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                // 消费者属性
                consumeMessageContext.setProps(new HashMap<>());
                // 消息队列
                consumeMessageContext.setMq(messageQueue);
                // 消费的消息
                consumeMessageContext.setMsgList(msgs);
                // 标记尚未成功
                consumeMessageContext.setSuccess(false);
                // 执行消息消费前的生命周期回调
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        // 更新消息的开始消费时间戳
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                // 回调监听器的消费方法，并保留消费状态
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                // 消费异常，写入warn日志，并更新存在异常的标记
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}", UtilAll.exceptionSimpleDesc(e), ConsumeMessageConcurrentlyService.this.consumerGroup, msgs, messageQueue, e);
                hasException = true;
            }
            // 计算消费耗时
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    // 存在异常时，返回异常状态
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    // 没有异常，状态为空，返回空状态
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                // 消费耗时超过了默认的15m，返回超时
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                // 消费重试，返回失败标识
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                // 消费成功，返回成功标识
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                // 如果存在消息者消息的钩子，则写入处理结果标识
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                // 消费者未范围消费状态时，写入warn日志
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}", ConsumeMessageConcurrentlyService.this.consumerGroup, msgs, messageQueue);
                // 更新为消费失败，避免为空后续出现的空指针异常
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                // 写入消费状态
                consumeMessageContext.setStatus(status.toString());
                // 写入是否消费成功
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
                // 执行消息消费后的生命周期回调
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            // 更新统计指标
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            if (!processQueue.isDropped()) {
                //处理消费结果
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}

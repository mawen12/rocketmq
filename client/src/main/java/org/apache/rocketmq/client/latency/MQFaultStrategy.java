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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo.QueueFilter;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * MQ的故障转移策略，提供从主题下选择消息队列的功能，默认实现调用{@link TopicPublishInfo#selectOneMessageQueue(String)}
 */
public class MQFaultStrategy {
    /**
     * 基于延迟的故障容错
     */
    private LatencyFaultTolerance<String> latencyFaultTolerance;
    /**
     * 是否开启延迟容错
     */
    private volatile boolean sendLatencyFaultEnable;
    /**
     * 是否启用开启检测器
     */
    private volatile boolean startDetectorEnable;
    /**
     * 延迟的数据
     */
    private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};
    /**
     * 不可用的间隔
     */
    private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};

    /**
     * 基于broker名称的队列过滤器，即过滤不在当前broker上的队列
     */
    public static class BrokerFilter implements QueueFilter {
        private String lastBrokerName;

        public void setLastBrokerName(String lastBrokerName) {
            this.lastBrokerName = lastBrokerName;
        }

        @Override
        public boolean filter(MessageQueue mq) {
            if (lastBrokerName != null) {
                /**
                 * 过滤不在当前broker上的队列
                 */
                return !mq.getBrokerName().equals(lastBrokerName);
            }
            return true;
        }
    }

    /**
     * 线程本地过滤器
     *
     * TODO by mawen 使用lambda提升可读性
     */
    private ThreadLocal<BrokerFilter> threadBrokerFilter = ThreadLocal.withInitial(BrokerFilter::new);

    private QueueFilter reachableFilter = new QueueFilter() {
        @Override
        public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isReachable(mq.getBrokerName());
        }
    };

    private QueueFilter availableFilter = new QueueFilter() {
        @Override
        public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isAvailable(mq.getBrokerName());
        }
    };


    public MQFaultStrategy(ClientConfig cc, Resolver fetcher, ServiceDetector serviceDetector) {
        this.latencyFaultTolerance = new LatencyFaultToleranceImpl(fetcher, serviceDetector);
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
    }

    // For unit test.
    public MQFaultStrategy(ClientConfig cc, LatencyFaultTolerance<String> tolerance) {
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
        this.latencyFaultTolerance = tolerance;
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
    }


    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public QueueFilter getAvailableFilter() {
        return availableFilter;
    }

    public QueueFilter getReachableFilter() {
        return reachableFilter;
    }

    public ThreadLocal<BrokerFilter> getThreadBrokerFilter() {
        return threadBrokerFilter;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
        this.latencyFaultTolerance.setStartDetectorEnable(startDetectorEnable);
    }

    public void startDetector() {
        this.latencyFaultTolerance.startDetector();
    }

    public void shutdown() {
        this.latencyFaultTolerance.shutdown();
    }

    /**
     * 选择一个主题下的消息队列并返回
     *
     * @param tpInfo
     * @param lastBrokerName
     * @param resetIndex
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName, final boolean resetIndex) {
        /**
         * 获取基于broker名称的过滤器
         */
        BrokerFilter brokerFilter = threadBrokerFilter.get();
        /**
         * 将消息队列上的broker更新上去
         */
        brokerFilter.setLastBrokerName(lastBrokerName);
        /**
         * 如果开启基于延迟的容错
         */
        if (this.sendLatencyFaultEnable) {
            /**
             * 重置索引，即从头开始找
             */
            if (resetIndex) {
                tpInfo.resetIndex();
            }
            /**
             * 使用可用和排除指定broker的过滤器配合轮询机制来查找消息队列
             */
            MessageQueue mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }
            /**
             * 基于可达到和排除指定broker的过滤器配合轮询机制来查找消息队列
             */
            mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }

            /**
             * 使用轮询机制选择下一个
             */
            return tpInfo.selectOneMessageQueue();
        }

        /**
         * 仅基于排除broker的过滤器配合轮询机制来查找消息队列
         */
        MessageQueue mq = tpInfo.selectOneMessageQueue(brokerFilter);
        if (mq != null) {
            return mq;
        }
        /**
         * 使用轮询机制选择下一个
         */
        return tpInfo.selectOneMessageQueue();
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation, final boolean reachable) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return this.notAvailableDuration[i];
            }
        }

        return 0;
    }
}

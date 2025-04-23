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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store.queue;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.ImportantPoint;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

/**
 * 用于操作consume queue中偏移量的组件
 *
 * <p>主要维护了以下三种类型的consume queue
 * <ul>
 *     <li>{@link org.apache.rocketmq.common.attribute.CQType#SimpleCQ}</li>
 *     <li>{@link org.apache.rocketmq.common.attribute.CQType#BatchCQ}</li>
 *     <li>{@code LMQ}</li>
 * </ul>
 */
@ImportantPoint("队列的偏移量，就是队列中消息数量")
public class QueueOffsetOperator {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 维护了{@link org.apache.rocketmq.common.attribute.CQType#SimpleCQ}类型的consume queue的偏移量信息
     */
    private ConcurrentMap<String/* topic-queueId */, Long/* 该主题队列下的消息总数，也被看作队列偏移量 */> topicQueueTable = new ConcurrentHashMap<>(1024);

    /**
     * 维护了{@link org.apache.rocketmq.common.attribute.CQType#BatchCQ}类型的consume queue的偏移量信息
     */
    private ConcurrentMap<String/* topic-queueId */, Long/* 该主题队列下的消息总数，也被看作队列偏移量 */> batchTopicQueueTable = new ConcurrentHashMap<>(1024);

    /**
     * 维护了{@code LMQ}类型的consume queue的偏移量信息
     */
    private ConcurrentMap<String/* topic-queueId */, Long/* 该主题队列下的消息总数，也被看作队列偏移量 */> lmqTopicQueueTable = new ConcurrentHashMap<>(1024);

    public long getQueueOffset(String topicQueueKey) {
        /**
         * 获取指定主题队列的消息总数，即队列偏移量
         */
        return ConcurrentHashMapUtils.computeIfAbsent(this.topicQueueTable, topicQueueKey, k -> 0L);
    }

    public Long getTopicQueueNextOffset(String topicQueueKey) {
        return this.topicQueueTable.get(topicQueueKey);
    }

    public void increaseQueueOffset(String topicQueueKey, short messageNum) {
        /**
         * 获取当前topic-queueId对应的消息总数，即队列偏移量
         */
        Long queueOffset = ConcurrentHashMapUtils.computeIfAbsent(this.topicQueueTable, topicQueueKey, k -> 0L);
        /**
         * 增加队列中消息总数，即队列偏移量
         */
        topicQueueTable.put(topicQueueKey, queueOffset + messageNum);
    }

    public void updateQueueOffset(String topicQueueKey, long offset) {
        this.topicQueueTable.put(topicQueueKey, offset);
    }

    public long getBatchQueueOffset(String topicQueueKey) {
        return ConcurrentHashMapUtils.computeIfAbsent(this.batchTopicQueueTable, topicQueueKey, k -> 0L);
    }

    public void increaseBatchQueueOffset(String topicQueueKey, short messageNum) {
        Long batchQueueOffset = ConcurrentHashMapUtils.computeIfAbsent(this.batchTopicQueueTable, topicQueueKey, k -> 0L);
        this.batchTopicQueueTable.put(topicQueueKey, batchQueueOffset + messageNum);
    }

    public long getLmqOffset(String topic, int queueId, OffsetInitializer callback) throws ConsumeQueueException {
        Preconditions.checkNotNull(callback, "ConsumeQueueOffsetCallback cannot be null");
        String topicQueue = topic + "-" + queueId;
        if (!lmqTopicQueueTable.containsKey(topicQueue)) {
            // Load from RocksDB on cache miss.
            Long prev = lmqTopicQueueTable.putIfAbsent(topicQueue, callback.maxConsumeQueueOffset(topic, queueId));
            if (null != prev) {
                log.error("[BUG] Data racing, lmqTopicQueueTable should NOT contain key={}", topicQueue);
            }
        }
        return lmqTopicQueueTable.get(topicQueue);
    }

    public void increaseLmqOffset(String topic, int queueId, short delta) throws ConsumeQueueException {
        String topicQueue = topic + "-" + queueId;
        if (!this.lmqTopicQueueTable.containsKey(topicQueue)) {
            throw new ConsumeQueueException(String.format("Max offset of Queue[name=%s, id=%d] should have existed", topic, queueId));
        }
        long prev = lmqTopicQueueTable.get(topicQueue);
        this.lmqTopicQueueTable.compute(topicQueue, (k, offset) -> offset + delta);
        long current = lmqTopicQueueTable.get(topicQueue);
        log.debug("Max offset of LMQ[{}:{}] increased: {} --> {}", topic, queueId, prev, current);
    }

    /**
     * @param topicQueueKey topic-queueId
     * @return 返回指定topic-queueId下当前的队列偏移量
     */
    public long currentQueueOffset(String topicQueueKey) {
        Long currentQueueOffset = this.topicQueueTable.get(topicQueueKey);
        // TODO by mawen simplify by getOrDefault
        return currentQueueOffset == null ? 0L : currentQueueOffset;
    }

    public synchronized void remove(String topic, Integer queueId) {
        String topicQueueKey = topic + "-" + queueId;
        // Beware of thread-safety
        this.topicQueueTable.remove(topicQueueKey);
        this.batchTopicQueueTable.remove(topicQueueKey);
        this.lmqTopicQueueTable.remove(topicQueueKey);

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void setLmqTopicQueueTable(ConcurrentMap<String, Long> lmqTopicQueueTable) {
        ConcurrentMap<String, Long> table = new ConcurrentHashMap<String, Long>(1024);
        for (Map.Entry<String, Long> entry : lmqTopicQueueTable.entrySet()) {
            if (MixAll.isLmq(entry.getKey())) {
                table.put(entry.getKey(), entry.getValue());
            }
        }
        this.lmqTopicQueueTable = table;
    }

    public ConcurrentMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.batchTopicQueueTable = batchTopicQueueTable;
    }
}

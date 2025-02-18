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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.exception.StoreException;
import org.rocksdb.RocksDBException;

/**
 * 与%HOME/store/consumequeue的相关操作接口
 */
public interface ConsumeQueueStoreInterface {

    /**
     * 开启consumequeue存储
     */
    void start();

    /**
     * 从$HOME/store/consumequeue下中加载文件
     *
     * @return {@code true}代表加载成功
     */
    boolean load();

    /**
     * 加载后摧毁%HOME/store/consumequeue下的文件
     */
    boolean loadAfterDestroy();

    /**
     * 从$HOME/store/consumequeue下的文件中恢复
     */
    void recover();

    /**
     * 从$HOME/store/consumequeue下的文件中并发恢复
     *
     * @return {@code true}恢复成功
     */
    boolean recoverConcurrently();

    /**
     * 停止consumequeue存储
     *
     * @return {@code true}停止成功
     */
    boolean shutdown();

    /**
     * 摧毁$HOME/store/consumequeue下的所有文件
     */
    void destroy();

    /**
     * 摧毁特定的consumequeue
     *
     * @throws RocksDBException only in rocksdb mode
     */
    void destroy(ConsumeQueueInterface consumeQueue) throws RocksDBException;

    /**
     * 将page cache刷新到磁盘文件
     *
     * @param consumeQueue 被刷新的consumequeue
     * @param flushLeastPages  被刷新的page cache的最小数量
     * @return {@code true}任何数据被刷新
     */
    boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages);

    /**
     * 将所有内置的consume queue刷新到磁盘
     *
     * @throws StoreException if there is an error during flush
     */
    void flush() throws StoreException;

    /**
     * 从指定commitlog的物理偏移量开始清理consumequeue中的过期数据
     *
     * <p>这种一般用于commitlog清理了过期的数据，此时也需要将consumequeue中对应的文件清理
     *
     * @param minCommitLogOffset 最小的commitlog物理偏移量
     */
    void cleanExpired(long minCommitLogOffset);

    /**
     * 检查consumequeue文件
     */
    void checkSelf();

    /**
     * 删除截至到最小的commitlog位置的过期文件
     *
     * @param consumeQueue 待删除的消费者队列
     * @param minCommitLogOffset 最小的commitlog偏移量
     * @return 被删除文件的数量
     */
    int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogOffset);

    /**
     * $HOME/store/consumequeue中第一个文件是否可用
     *
     * @param consumeQueue
     * @return {@code true}存在第一个文件
     */
    boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue);

    /**
     * $HOME/store/consumequeue中第一个文件是否存在
     *
     * @param consumeQueue
     * @return {@code true}存在第一个文件
     */
    boolean isFirstFileExist(ConsumeQueueInterface consumeQueue);

    /**
     * 滚动到$HOME/store/consuequeue的下一个文件的偏移量
     *
     *
     * @param consumeQueue
     * @param offset 下一个开始的偏移量
     * @return 下一个文件的开始偏移量
     */
    long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset);

    /**
     * 截断脏数据
     *
     * @param offsetToTruncate 要截断的偏移量
     * @throws RocksDBException only in rocksdb mode
     */
    void truncateDirty(long offsetToTruncate) throws RocksDBException;

    /**
     * 应用{@link DispatchRequest}并构造消费者队列。
     *
     * <p>该方法应该是幂等的。
     *
     * @param consumeQueue consume queue
     * @param request dispatch request
     */
    void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request);

    /**
     * 应用{@link DispatchRequest}并构造消费者队列
     *
     * <p>该方法是幂等的。
     *
     * @param request dispatch request
     * @throws RocksDBException only in rocksdb mode will throw exception
     */
    void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException;

    /**
     * range query cqUnit(ByteBuffer) in rocksdb
     * 从$HOME/store/consumequeue/<topic>/<queueId>下查询，
     * 从指定索引开始，最大num条的consumequeue unit，最终以
     * {@link ByteBuffer}列表返回.
     *
     * @param topic 主题
     * @param queueId 队列ID
     * @param startIndex 开始索引
     * @param num 消息数量
     * @return the byteBuffer list of the topic-queueId in rocksdb
     * @throws RocksDBException only in rocksdb mode
     */
    List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException;

    /**
     * @param topic 主题
     * @param queueId 队列ID
     * @param startIndex 开始索引
     * @return 返回$HOME/store/consumequeue/<topic>/<queueId>下，从指定索引开始的一条消息，以{@link ByteBuffer}返回
     * @throws RocksDBException only in rocksdb mode
     */
    ByteBuffer get(final String topic, final int queueId, final long startIndex) throws RocksDBException;

    /**
     * @return 返回consumequeue表
     */
    ConcurrentMap<String/* 主题 */, ConcurrentMap<Integer/* 队列ID */, ConsumeQueueInterface/* 代表consumequeue的接口 */>> getConsumeQueueTable();

    /**
     * 分配队列偏移量
     *
     * @param msg 消息本身
     * @throws RocksDBException only in rocksdb mode
     */
    void assignQueueOffset(MessageExtBrokerInner msg) throws RocksDBException;

    /**
     * 增加队列偏移量
     *
     * @param msg 消息本身
     * @param messageNum 消息数量
     */
    void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum);

    /**
     * 增加lmq队列偏移量
     *
     * @param topic 主题
     * @param queueId 队列ID
     * @param delta 增加的数量
     */
    void increaseLmqOffset(String topic, int queueId, short delta) throws ConsumeQueueException;

    /**
     * @param topic 主题
     * @param queueId 队列ID
     * @return 返回lmq队列偏移量
     */
    long getLmqQueueOffset(String topic, int queueId) throws ConsumeQueueException;

    /**
     * 根据最小的物理偏移量恢复主题队列表
     *
     * @param minPhyOffset
     */
    void recoverOffsetTable(long minPhyOffset);

    /**
     * 设置主题队列表
     *
     * @param topicQueueTable
     */
    void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable);

    /**
     * 移除指定主题队列表
     *
     * @param topic 主题
     * @param queueId 队列ID
     */
    void removeTopicQueueTable(String topic, Integer queueId);

    /**
     * @return 返回主题队列表，key为主题，value为队列ID
     */
    // TODO by mawen the returned value should be same as setTopicQueueTable's parameter
    ConcurrentMap getTopicQueueTable();

    /**
     * @param topic 主题
     * @param queueId 队列ID
     * @return 返回$HOME/store/<topic>/<queueId>下消息的最大物理偏移量
     */
    Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId);

    /**
     * @param topic 主题名称
     * @param queueId 队列ID
     * @return 返回$HOME/store/<topic>/<queueId>下最大的消息偏移量
     * @throws ConsumeQueueException if there is an error while retrieving max consume queue offset
     */
    Long getMaxOffset(String topic, int queueId) throws ConsumeQueueException;

    /**
     * @return 返回$HOME/store下最大的物理偏移量
     * @throws RocksDBException only in rocksdb mode
     */
    long getMaxPhyOffsetInConsumeQueue() throws RocksDBException;

    /**
     * @param topic 主题
     * @param queueId 队列ID
     * @return 返回$HOME/store/<topic>/<queueId>下消息的最小偏移量
     * @throws RocksDBException only in rocksdb mode
     */
    long getMinOffsetInQueue(final String topic, final int queueId) throws RocksDBException;

    /**
     * @param topic 主题
     * @param queueId 队列ID
     * @return 返回$HOME/store/<topic>/<queueId>下消息的最大偏移量
     * @throws RocksDBException only in rocksdb mode
     */
    long getMaxOffsetInQueue(final String topic, final int queueId) throws RocksDBException;

    /**
     * 返回$HOME/store/<topic>/<queueId>下，<=timestamp或>=timestamp的逻辑偏移量
     *
     * @param timestamp    时间戳
     * @param boundaryType <= 或 >=
     * @return consumequeue 逻辑偏移量
     * @throws RocksDBException only in rocksdb mode
     */
    long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) throws RocksDBException;

    /**
     * 如果存在，直接返回consumequeue，反之则创建再返回
     *
     * @param topic 主题
     * @param queueId 队列ID
     * @return the consumeQueue
     */
    ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId);

    /**
     * 根据主题查询consumequeue映射
     *
     * @param topic 主题
     * @return the consumeQueueMap of topic
     */
    ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface/* 代表了$HOME/store/consumequeue/<topic>/<queueId>/文件 */> findConsumeQueueMap(String topic);

    /**
     * get the total size of all consumeQueue
     * @return 返回所有的$HOME/store/consumequeue的总大小
     */
    long getTotalSize();

    /**
     * @param cqUnit
     * @return 根据{@link CqUnit}从commitlog中查询消息的存储时间
     */
    long getStoreTime(CqUnit cqUnit);
}

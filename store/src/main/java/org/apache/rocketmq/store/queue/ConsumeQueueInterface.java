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

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;
import org.rocksdb.RocksDBException;

/**
 * 用于操作$HOME/store/consumequeue/<topic>/<queueId>下文件的接口
 */
public interface ConsumeQueueInterface extends FileQueueLifeCycle {
    /**
     * @return 返回该cq所属的主题名称
     */
    String getTopic();

    /**
     * @return 返回该cq所属的队列ID
     */
    int getQueueId();

    /**
     * Get the units from the start offset.
     * 从开始索引获取cqUnit
     *
     * @param startIndex 开始索引
     * @return 从哪里开始迭代
     */
    ReferredIterator<CqUnit> iterateFrom(long startIndex);

    /**
     * 从开始索引获取指定总数的cqUnit
     *
     * @param startIndex 开始索引
     * @param count 被迭代的cqUnit总数
     * @return 从哪里开始迭代
     * @throws RocksDBException only in rocksdb mode
     */
    ReferredIterator<CqUnit> iterateFrom(long startIndex, int count) throws RocksDBException;

    /**
     * @param index 逻辑索引
     * @return 获取特定索引的cqUnit
     */
    CqUnit get(long index);

    /**
     * @return 获取特定索引处的cqUnit和消息存储时间
     */
    Pair<CqUnit, Long> getCqUnitAndStoreTime(long index);

    /**
     * @return 获取最早的cqUnit和消息存储时间
     */
    Pair<CqUnit, Long> getEarliestUnitAndStoreTime();

    /**
     * @return 获取最早的cqUnit
     */
    CqUnit getEarliestUnit();

    /**
     * @return 获取最新的cqUnit
     */
    CqUnit getLatestUnit();

    /**
     * @return 获取最新的commitlog的物理偏移量
     */
    long getLastOffset();

    /**
     * @return 获取consumequeue中最小的索引
     */
    long getMinOffsetInQueue();

    /**
     * @return 获取consumequeue中最大的索引
     */
    long getMaxOffsetInQueue();

    /**
     * @return 获取consumequeue的消息总数
     */
    long getMessageTotalInQueue();

    /**
     * @param timestamp 时间戳
     * @return 获取最贴近给定时间戳的consumequeue的逻辑偏移量
     */
    long getOffsetInQueueByTime(final long timestamp);

    /**
     * @param timestamp    时间戳
     * @param boundaryType 更多或更高
     * @return 获取最贴近给定时间戳的consumequeue的逻辑偏移量，具体取上还是取下由{@link BoundaryType}来决定
     */
    long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType);

    /**
     * @return 返回$HOME/store/consumequeue中最大的commitlog的物理偏移量
     */
    long getMaxPhysicOffset();

    /**
     * 通常cqUnit文件与commitlog并不完全一致，因为在第一个cq文件中可能存在冗余数据。
     *
     * @return cq文件中最小的有效位置
     */
    long getMinLogicOffset();

    /**
     * @return consumequeue类型
     */
    CQType getCQType();

    /**
     * @return 获取consume queue文件在磁盘上的占用大小
     */
    long getTotalSize();

    /**
     * @return 获取此consume queue的单元大小，不同的consume queue实现中单元大小不同
     */
    int getUnitSize();

    /**
     * 通过最小commitlog偏移量来纠正最小偏移量
     *
     * @param minCommitLogOffset 最小的commitlog物理偏移量
     */
    void correctMinOffset(long minCommitLogOffset);

    /**
     * 分发来自commitlog的请求，用于构建consume queue
     *
     * @param request 包含分发信息的请求
     */
    void putMessagePositionInfoWrapper(DispatchRequest request);

    /**
     * 分配consume queue偏移量
     *
     * @param queueOffsetAssigner 负责执行consume queue偏移量分配的代理类
     * @param msg 消息本身
     * @throws RocksDBException only in rocksdb mode
     */
    void assignQueueOffset(QueueOffsetOperator queueOffsetAssigner, MessageExtBrokerInner msg) throws RocksDBException;

    /**
     * 自增consume queue偏移量
     *
     * @param queueOffsetAssigner 负责执行consume queue偏移量分配的代理类
     * @param msg 消息本身
     * @param messageNum 消息数量
     */
    void increaseQueueOffset(QueueOffsetOperator queueOffsetAssigner, MessageExtBrokerInner msg, short messageNum);

    /**
     * 估计与给定过滤器匹配的消息记录数
     *
     * @param from 包含
     * @param to 包含
     * @param filter 特定的消息过滤器
     * @return 匹配的消息记录数
     */
    long estimateMessageCount(long from, long to, MessageFilter filter);
}

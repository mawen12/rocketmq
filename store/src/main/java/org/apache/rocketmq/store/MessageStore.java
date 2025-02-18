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
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.PerfCounter;
import org.rocksdb.RocksDBException;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;

/**
 * 消息存储的接口定义，其提供了消息存储相关的核心逻辑，其允许第三方供应商提供自定义的消息存储。
 */
public interface MessageStore {

    /**
     * 将之前存储的消息加载到内存中
     *
     * @return 加载成功返回true，否则返回false
     */
    boolean load();

    /**
     * 启动当前消息存储
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * 停止消息存储
     */
    void shutdown();

    /**
     * 销毁消息存储，通常在调用该方法后，所有持久化的文件都需要被移除（或删除）
     */
    void destroy();

    /**
     * 以异步方式将消息保存到存储中。处理器可以继续处理下一个请求而不是等待结果，
     * 当消息保存成功后，会以异步的方式通知客户端
     *
     * @param msg 要存储的消息实例
     * @return 代表存储操作结果的CompletableFuture对象
     */
    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        return CompletableFuture.completedFuture(putMessage(msg));
    }

    /**
     * 以异步的方式将一批消息保存到存储中。处理器可以处理下一个请求而不是等待结果。
     * 当消息保存成功后，会以异步的方式通知客户端
     *
     * @param messageExtBatch 要存储的消息批次实例
     * @return 代表存储操作结果的CompletableFuture对象
     */
    default CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        return CompletableFuture.completedFuture(putMessages(messageExtBatch));
    }

    /**
     * 以同步的方式将消息保存到存储中。
     *
     * @param msg 要存储的消息实例
     * @return 存储操作结果
     */
    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    /**
     * 以同步的方式将一批消息保存到存储中
     *
     * @param messageExtBatch 要存储的消息批次实例
     * @return 存储批次消息的结果
     */
    PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

    /**
     * 查询指定主题下的指定队列ID中从给定offset开始，最多{@code maxMsgNums}条消息。
     * 对于从底层查询出来的结果，将使用{@code messageFilter}对消息进一步过滤。
     *
     * <p>执行逻辑为先获取指定数量的消息，再对消息进行过滤
     *
     * <p>对于该方法需要注意，返回的结果数量最大为{@code maxMsgNums}，因为提供的消息过滤器可能会过滤掉消息
     *
     * @param group         加载该查询的消费者分组
     * @param topic         要查询的主题
     * @param queueId       要查询的队列ID
     * @param offset        开始查询的逻辑偏移量位置
     * @param maxMsgNums    要查询的消息最大数量
     * @param messageFilter 用于展示所需消息的过滤器
     * @return 匹配的消息
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * {@link #getMessage(String, String, int, long, int, MessageFilter)}的异步版本
     *
     * @param group         加载该查询的消费者分组
     * @param topic         要查询的主题
     * @param queueId       要查询的队列ID
     * @param offset        开始查询的逻辑偏移量位置
     * @param maxMsgNums    要查询的消息最大数量
     * @param messageFilter 用于展示所需消息的过滤器
     * @return 匹配的消息
     */
    CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * 查询指定主题下的指定队列ID中从给定offset开始，最多{@code maxMsgNums}条消息且最大{@code maxTotalMsgSize}。
     * 对于从底层查询出来的结果，将使用{@code messageFilter}对消息进一步过滤。
     *
     * <p>执行逻辑为先获取指定数量且指定大小的消息，再对消息进行过滤。
     *
     * <p>对于该方法需要注意，返回的结果数量最大为{@code maxMsgNums}，因为提供的消息过滤器可能会过滤掉消息
     *
     * <p>该方法是{@link #getMessage(String, String, int, long, int, int, MessageFilter)}的增强版，
     * 可以进一步准确限制返回消息的大小
     *
     * @param group           加载该查询的消费者分组
     * @param topic           要查询的主题
     * @param queueId         要查询的队列ID
     * @param offset          开始查询的逻辑偏移量位置
     * @param maxMsgNums      要查询的消息最大数量
     * @param maxTotalMsgSize 要查询的消息最大字节
     * @param messageFilter   用于展示所需消息的过滤器
     * @return 匹配的消息
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId,
                                final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

    /**
     * {@link #getMessage(String, String, int, long, int, int, MessageFilter)}的异步版本
     *
     * @param group           加载该查询的消费者分组
     * @param topic           要查询的主题
     * @param queueId         要查询的队列ID
     * @param offset          开始查询的逻辑偏移量位置
     * @param maxMsgNums      要查询的消息最大数量
     * @param maxTotalMsgSize 要查询的消息最大字节
     * @param messageFilter   用于展示所需消息的过滤器
     * @return 匹配的消息
     */
    CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId,
                                                        final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

    /**
     * 获取指定主题下指定队列ID下，最大的消息偏移量
     *
     * @param topic   主题名称
     * @param queueId 队列ID
     * @return 目前最大的偏移量
     */
    long getMaxOffsetInQueue(final String topic, final int queueId) throws ConsumeQueueException;

    /**
     * 获取指定主题下指定队列ID下，已提交或未提交的最大的消息偏移量
     *
     * <p>{@code committed}用于控制读取的内容来源是consumequeue还是commitlog
     * <ul>
     *     <li>{@code true}，代表获取以消费的最大的消息偏移量，该数据是存储在$HOME/store/consumequeue/<topic>/<queueId>/下的</li>
     *     <li>{@code false}，代表获取未消费的最大的消息偏移量，该数据是存储在$HOME/store/commitlog下的</li>
     * </ul>
     *
     * @param topic     主题名称
     * @param queueId   队列ID
     * @param committed {@code true}代表读取consumequeue，{@code false}代表读取commitlog
     * @return 目前最大的偏移量
     */
    long getMaxOffsetInQueue(final String topic, final int queueId, final boolean committed) throws ConsumeQueueException;

    /**
     * 获取指定主题下指定队列ID下最小的消息偏移量
     *
     * @param topic   主题名称
     * @param queueId 队列ID
     * @return 目前最小的偏移量
     */
    long getMinOffsetInQueue(final String topic, final int queueId);

    /**
     * @return 返回定时器消息存储
     */
    TimerMessageStore getTimerMessageStore();

    /**
     * 设置定时器消息存储
     *
     * @param timerMessageStore 定时器消息存储
     */
    void setTimerMessageStore(TimerMessageStore timerMessageStore);

    /**
     * 获取在commitlog中指定主题下指定队列ID，指定消费队列偏移量的物理消息偏移量.
     *
     * @param topic              消息所属的主题
     * @param queueId            队列ID
     * @param consumeQueueOffset 消费队列的偏移量
     * @return 物理偏移量
     */
    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 获取指定主题下指定队列ID，指定时间戳的消息的物理偏移量
     *
     * @param topic     消息所属的主题
     * @param queueId   队列ID
     * @param timestamp 时间戳
     * @return 物理偏移量
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    /**
     * 获取指定主题下指定队列ID，指定边界的时间戳的物理偏移量
     *
     * @param topic        消息的主题
     * @param queueId      队列ID
     * @param timestamp    时间戳
     * @param boundaryType 时间戳的边界
     * @return 物理偏移量
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp, final BoundaryType boundaryType);

    /**
     * 根据给定commitlog的偏移量查询一条消息
     *
     * @param commitLogOffset 物理偏移量
     * @return 匹配偏移量的消息
     */
    MessageExt lookMessageByOffset(final long commitLogOffset);

    /**
     * 根据给定commitlog的偏移量和消息大小查询一条消息
     *
     * @param commitLogOffset 物理偏移量
     * @param size            消息大小
     * @return 匹配偏移量和大小的消息
     */
    MessageExt lookMessageByOffset(long commitLogOffset, int size);

    /**
     * 根据给定commitlog的偏移量查询一条消息
     *
     * @param commitLogOffset 物理偏移量
     * @return 消息结果的包装
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    /**
     * 根据给定commitlog的偏移量查询一条消息
     *
     * @param commitLogOffset 物理偏移量
     * @param msgSize         消息大小
     * @return 消息结果的包装
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    /**
     * 获取存储的运行时信息
     *
     * @return 消息存储的运行信息
     */
    String getRunningDataInfo();

    long getTimingMessageCount(String topic);

    /**
     * 消息存储的运行信息，通常包含多个统计信息
     *
     * @return 以key-value键值对保存的消息存储的运行时信息
     */
    HashMap<String, String> getRuntimeInfo();

    /**
     * @return high available 运行时信息
     */
    HARuntimeInfo getHARuntimeInfo();

    /**
     * @return 返回commitlog中最新消息的偏移量
     */
    long getMaxPhyOffset();

    /**
     * 由于rocketmq存在过期文件删除的机制，默认为48小时有效期，
     * 因此最早的消息一般为48小时之前的消息，而非实际场景中最早
     * 生产的消息。
     *
     * @return 返回commitlog中最早消息的偏移量
     */
    long getMinPhyOffset();

    /**
     * 由于rocketmq存在过期文件删除的机制，默认为48小时有效期，
     * 因此最早的消息一般为48小时之前的消息，而非实际场景中最早
     * 生产的消息。
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @return consumequeue最早消息的存储时间
     */
    long getEarliestMessageTime(final String topic, final int queueId);

    /**
     * 由于rocketmq存在过期文件删除的机制，默认为48小时有效期，
     * 因此最早的消息一般为48小时之前的消息，而非实际场景中最早
     * 生产的消息。
     *
     * @return 当前存储中最早的消息的存储时间
     */
    long getEarliestMessageTime();

    /**
     * {@link #getEarliestMessageTime(String, int)}的异步版本。
     *
     * @return 当前存储中最早的消息的存储时间
     */
    CompletableFuture<Long> getEarliestMessageTimeAsync(final String topic, final int queueId);

    /**
     * @param topic              主题
     * @param queueId            队列ID
     * @param consumeQueueOffset consumequeue中的偏移量
     * @return 消息的存储时间
     */
    long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * {@link #getMessageStoreTimeStamp(String, int, long)}的异步版本
     *
     * @param topic              主题
     * @param queueId            队列ID
     * @param consumeQueueOffset consumequeue的偏移量
     * @return 消息的存储时间
     */
    CompletableFuture<Long> getMessageStoreTimeStampAsync(final String topic, final int queueId,
                                                          final long consumeQueueOffset);

    /**
     * 由于consumequeue中的entry定长设计的，每个entry占用20个字节，由以下组成：8字节的commitlog物理偏移量，4字节的消息长度，8字节的标签hash值。
     * 因此只要读取到consumequeue的大小fileSize，那么个数就等于fileSize/20，依次计算所有的consumequeue文件，累加得到总数。
     *
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @return consumequeue中总的消息数量
     */
    long getMessageTotalInQueue(final String topic, final int queueId);

    /**
     * 返回在commitlog中给以给定偏移量作为开始位置的原始数据。
     *
     * <p>一般用于副本，进行数据同步使用。
     *
     * @param offset 开始偏移量
     * @return commitlog中的数据
     */
    SelectMappedBufferResult getCommitLogData(final long offset);

    /**
     * 返回在多个commtilog中以给定偏移量作为开始位置的原始数据。
     *
     * <p>一般用于副本，进行数据同步使用。
     *
     * @param offset 开始偏移量
     * @param size   返回数据的大小
     * @return commitlog中的数据
     */
    List<SelectMappedBufferResult> getBulkCommitLogData(final long offset, final int size);

    /**
     * 将数组中的消息追加到commitlog中
     *
     * @param startOffset commitlog中开始的偏移量
     * @param data        要追加的数据
     * @param dataStart   数组中开始的偏移量
     * @param dataLength  数组的长度
     * @return {@code true}追加成功，{@code false}追加失败
     */
    boolean appendToCommitLog(final long startOffset, final byte[] data, int dataStart, int dataLength);

    /**
     * 手动执行文件删除
     */
    void executeDeleteFilesManually();

    /**
     * 使用指定主题下查询指定时间范围内，满足指定key的最大数量的消息
     *
     * @param topic  主题
     * @param key    消息key
     * @param maxNum 可能的最大数量的消息
     * @param begin  开始时间戳
     * @param end    结束时间戳
     */
    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
                                    final long end);

    /**
     * {@link #queryMessage(String, String, int, long, long)}的异步版本
     *
     * @param topic  主题
     * @param key    消息key
     * @param maxNum 可能消息的最大数量
     * @param begin  开始时间戳
     * @param end    结束时间戳
     */
    CompletableFuture<QueryMessageResult> queryMessageAsync(final String topic, final String key, final int maxNum,
                                                            final long begin, final long end);

    /**
     * 更新master的high available地址
     *
     * @param newAddr 新的地址
     */
    void updateHaMasterAddress(final String newAddr);

    /**
     * 更新master的地址
     *
     * @param newAddr 新的地址
     */
    void updateMasterAddress(final String newAddr);

    /**
     * @return 返回slave落后了多少字节数
     */
    long slaveFallBehindMuch();

    /**
     * @return 返回存储的当前时间戳，从1970-01-01开始计算
     */
    long now();

    /**
     * 删除主题下的consumequeue的文件和未使用的统计数据。
     *
     * <p>允许用户删除系统主题
     *
     * <p>删除的文件位于$HOME/store/consumequeue/目录下
     *
     * @param deleteTopics 未使用的主题名称集合
     * @return 被删除的主题的数量
     */
    int deleteTopics(final Set<String> deleteTopics);

    /**
     * 清理不包含指定主题集合外的其他主题
     *
     * <p>删除的文件位于$HOME/store/consumequeue/目录下
     *
     * @param retainTopics 所有合法的而主题
     * @return 被删除的主题的数量
     */
    int cleanUnusedTopic(final Set<String> retainTopics);

    /**
     * 清理已过期的consumequeue文件
     */
    void cleanExpiredConsumerQueue();

    /**
     * 检查consumequeue中指定主题下的指定队列ID下特定的consumeOffset对应的消息
     * 是否不在内存中
     *
     * @param topic         主题
     * @param queueId       队列ID
     * @param consumeOffset consumequeue偏移量
     * @return {@code true}如果消息不在内存中，{@code false}则代表在磁盘上
     * @deprecated As of RIP-57, replaced by {@link #checkInMemByConsumeOffset(String, int, long, int)}, see <a href="https://github.com/apache/rocketmq/issues/5837">this issue</a> for more details
     */
    @Deprecated
    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * 检查consumequeue中指定主题下的指定队列ID下特定的consumeOffset开始，
     * 最大批次大小的消息，是否不在page cache中
     *
     * @param topic         主题
     * @param queueId       队列ID
     * @param consumeOffset consumequeue偏移量
     * @return {@code true}如果消息在page cache中，{@code false}则代表在磁盘上
     */
    boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize);

    /**
     * 检查consumequeue中指定主题下的指定队列ID下特定的consumeOffset对应的消息
     * 是否在存储中存在
     *
     * @param topic         主题
     * @param queueId       队列ID
     * @param consumeOffset consumequeue偏移量
     * @return {@code true}如果消息存在于store中，{@code false}不存在于store中
     */
    boolean checkInStoreByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * @return 获取已经保存到commitlog中，但是尚未dispatch到consumequeue的消息字节数
     */
    long dispatchBehindBytes();

    /**
     * @return 返回已经保存到commitlog中，但是尚未dispatch到consumequeue的毫秒数，
     * 即当前记录上次同步的时间差
     */
    long dispatchBehindMilliseconds();

    /**
     * 刷新store以便持久化所有数据
     *
     * @return 被刷新到持久化存储设备的最大物理偏移量
     */
    long flush();

    /**
     * @return 返回当前已被刷新的最大物理偏移量
     */
    long getFlushedWhere();

    /**
     * 重置写偏移量到指定的偏移量
     *
     * @param phyOffset 新的偏移量
     * @return {@code true}重置成功，{@code false}重置失败
     */
    boolean resetWriteOffset(long phyOffset);

    /**
     * @return 返回确认偏移量
     */
    long getConfirmOffset();

    /**
     * 更新设置偏移量到新的偏移量
     *
     * @param phyOffset 新的物理偏移量
     */
    void setConfirmOffset(long phyOffset);

    /**
     * 检查操作系统页page cache是否繁忙
     *
     * @return {@code true}OS page cache处于繁忙状态，{@code false}位处于繁忙
     */
    boolean isOSPageCacheBusy();

    /**
     * @return 获取到目前位置，在存储中以毫秒为单位的锁定时间
     */
    long lockTimeMills();

    /**
     * 检查瞬态存储池是否不足
     *
     * @return {@code true}瞬态存储池不足；{@code false}瞬态存储池充足
     */
    boolean isTransientStorePoolDeficient();

    /**
     * @return 返回commitlog dispatcher的列表
     */
    LinkedList<CommitLogDispatcher> getDispatcherList();

    /**
     * Add dispatcher.
     *
     * @param dispatcher commit log dispatcher to add
     */
    void addDispatcher(CommitLogDispatcher dispatcher);

    /**
     * Get consume queue of the topic/queue. If consume queue not exist, will return null
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueueInterface getConsumeQueue(String topic, int queueId);

    /**
     * Get consume queue of the topic/queue. If consume queue not exist, will create one then return it.
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueueInterface findConsumeQueue(String topic, int queueId);

    /**
     * Get BrokerStatsManager of the messageStore.
     *
     * @return BrokerStatsManager.
     */
    BrokerStatsManager getBrokerStatsManager();

    /**
     * Will be triggered when a new message is appended to commit log.
     *
     * @param msg           the msg that is appended to commit log
     * @param result        append message result
     * @param commitLogFile commit log file
     */
    void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile);

    /**
     * Will be triggered when a new dispatch request is sent to message store.
     *
     * @param dispatchRequest dispatch request
     * @param doDispatch      do dispatch if true
     * @param commitLogFile   commit log file
     * @param isRecover       is from recover process
     * @param isFileEnd       if the dispatch request represents 'file end'
     * @throws RocksDBException      only in rocksdb mode
     */
    void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile,
                             boolean isRecover, boolean isFileEnd) throws RocksDBException;

    /**
     * Get the message store config
     *
     * @return the message store config
     */
    MessageStoreConfig getMessageStoreConfig();

    /**
     * Get the statistics service
     *
     * @return the statistics service
     */
    StoreStatsService getStoreStatsService();

    /**
     * Get the store checkpoint component
     *
     * @return the checkpoint component
     */
    StoreCheckpoint getStoreCheckpoint();

    /**
     * Get the system clock
     *
     * @return the system clock
     */
    SystemClock getSystemClock();

    /**
     * Get the commit log
     *
     * @return the commit log
     */
    CommitLog getCommitLog();

    /**
     * Get running flags
     *
     * @return running flags
     */
    RunningFlags getRunningFlags();

    /**
     * Get the transient store pool
     *
     * @return the transient store pool
     */
    TransientStorePool getTransientStorePool();

    /**
     * Get the HA service
     *
     * @return the HA service
     */
    HAService getHaService();

    /**
     * Get the allocate-mappedFile service
     *
     * @return the allocate-mappedFile service
     */
    AllocateMappedFileService getAllocateMappedFileService();

    /**
     * Truncate dirty logic files
     *
     * @param phyOffset physical offset
     * @throws RocksDBException only in rocksdb mode
     */
    void truncateDirtyLogicFiles(long phyOffset) throws RocksDBException;

    /**
     * Unlock mappedFile
     *
     * @param unlockMappedFile the file that needs to be unlocked
     */
    void unlockMappedFile(MappedFile unlockMappedFile);

    /**
     * Get the perf counter component
     *
     * @return the perf counter component
     */
    PerfCounter.Ticks getPerfCounter();

    /**
     * Get the queue store
     *
     * @return the queue store
     */
    @Nonnull
    ConsumeQueueStoreInterface getQueueStore();

    /**
     * If 'sync disk flush' is configured in this message store
     *
     * @return yes if true, no if false
     */
    boolean isSyncDiskFlush();

    /**
     * If this message store is sync master role
     *
     * @return yes if true, no if false
     */
    boolean isSyncMaster();

    /**
     * Assign a message to queue offset. If there is a race condition, you need to lock/unlock this method
     * yourself.
     *
     * @param msg        message
     * @throws RocksDBException
     */
    void assignOffset(MessageExtBrokerInner msg) throws RocksDBException;

    /**
     * Increase queue offset in memory table. If there is a race condition, you need to lock/unlock this method
     *
     * @param msg        message
     * @param messageNum message num
     */
    void increaseOffset(MessageExtBrokerInner msg, short messageNum);

    /**
     * Get master broker message store in process in broker container
     *
     * @return
     */
    MessageStore getMasterStoreInProcess();

    /**
     * Set master broker message store in process
     *
     * @param masterStoreInProcess
     */
    void setMasterStoreInProcess(MessageStore masterStoreInProcess);

    /**
     * Use FileChannel to get data
     *
     * @param offset
     * @param size
     * @param byteBuffer
     * @return
     */
    boolean getData(long offset, int size, ByteBuffer byteBuffer);

    /**
     * Set the number of alive replicas in group.
     *
     * @param aliveReplicaNums number of alive replicas
     */
    void setAliveReplicaNumInGroup(int aliveReplicaNums);

    /**
     * Get the number of alive replicas in group.
     *
     * @return number of alive replicas
     */
    int getAliveReplicaNumInGroup();

    /**
     * Wake up AutoRecoverHAClient to start HA connection.
     */
    void wakeupHAClient();

    /**
     * Get master flushed offset.
     *
     * @return master flushed offset
     */
    long getMasterFlushedOffset();

    /**
     * Get broker init max offset.
     *
     * @return broker max offset in startup
     */
    long getBrokerInitMaxOffset();

    /**
     * Set master flushed offset.
     *
     * @param masterFlushedOffset master flushed offset
     */
    void setMasterFlushedOffset(long masterFlushedOffset);

    /**
     * Set broker init max offset.
     *
     * @param brokerInitMaxOffset broker init max offset
     */
    void setBrokerInitMaxOffset(long brokerInitMaxOffset);

    /**
     * Calculate the checksum of a certain range of data.
     *
     * @param from begin offset
     * @param to   end offset
     * @return checksum
     */
    byte[] calcDeltaChecksum(long from, long to);

    /**
     * Truncate commitLog and consume queue to certain offset.
     *
     * @param offsetToTruncate offset to truncate
     * @return true if truncate succeed, false otherwise
     * @throws RocksDBException only in rocksdb mode
     */
    boolean truncateFiles(long offsetToTruncate) throws RocksDBException;

    /**
     * Check if the offset is aligned with one message.
     *
     * @param offset offset to check
     * @return true if aligned, false otherwise
     */
    boolean isOffsetAligned(long offset);

    /**
     * Get put message hook list
     *
     * @return List of PutMessageHook
     */
    List<PutMessageHook> getPutMessageHookList();

    /**
     * Set send message back hook
     *
     * @param sendMessageBackHook
     */
    void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook);

    /**
     * Get send message back hook
     *
     * @return SendMessageBackHook
     */
    SendMessageBackHook getSendMessageBackHook();

    //The following interfaces are used for duplication mode

    /**
     * Get last mapped file and return lase file first Offset
     *
     * @return lastMappedFile first Offset
     */
    long getLastFileFromOffset();

    /**
     * Get last mapped file
     *
     * @param startOffset
     * @return true when get the last mapped file, false when get null
     */
    boolean getLastMappedFile(long startOffset);

    /**
     * Set physical offset
     *
     * @param phyOffset
     */
    void setPhysicalOffset(long phyOffset);

    /**
     * Return whether mapped file is empty
     *
     * @return whether mapped file is empty
     */
    boolean isMappedFilesEmpty();

    /**
     * Get state machine version
     *
     * @return state machine version
     */
    long getStateMachineVersion();

    /**
     * Check message and return size
     *
     * @param byteBuffer
     * @param checkCRC
     * @param checkDupInfo
     * @param readBody
     * @return DispatchRequest
     */
    DispatchRequest checkMessageAndReturnSize(final ByteBuffer byteBuffer, final boolean checkCRC,
                                              final boolean checkDupInfo, final boolean readBody);

    /**
     * Get remain transientStoreBuffer numbers
     *
     * @return remain transientStoreBuffer numbers
     */
    int remainTransientStoreBufferNumbs();

    /**
     * Get remain how many data to commit
     *
     * @return remain how many data to commit
     */
    long remainHowManyDataToCommit();

    /**
     * Get remain how many data to flush
     *
     * @return remain how many data to flush
     */
    long remainHowManyDataToFlush();

    /**
     * Get whether message store is shutdown
     *
     * @return whether shutdown
     */
    boolean isShutdown();

    /**
     * Estimate number of messages, within [from, to], which match given filter
     *
     * @param topic   Topic name
     * @param queueId Queue ID
     * @param from    Lower boundary of the range, inclusive.
     * @param to      Upper boundary of the range, inclusive.
     * @param filter  The message filter.
     * @return Estimate number of messages matching given filter.
     */
    long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter);

    /**
     * Get metrics view of store
     *
     * @return List of metrics selector and view pair
     */
    List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView();

    /**
     * Init store metrics
     *
     * @param meter                     opentelemetry meter
     * @param attributesBuilderSupplier metrics attributes builder
     */
    void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier);

    /**
     * Recover topic queue table
     */
    void recoverTopicQueueTable();

    /**
     * notify message arrive if necessary
     */
    void notifyMessageArriveIfNecessary(DispatchRequest dispatchRequest);
}

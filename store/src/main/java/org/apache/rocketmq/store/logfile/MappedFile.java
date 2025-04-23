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
package org.apache.rocketmq.store.logfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.apache.rocketmq.common.annotation.ImportantPoint;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;

/**
 * 映射文件，用于存储消息的物理文件，提供刷新、提供等功能
 *
 * <p>映射文件设计规则：
 * <ul>
 *     <li>
 *         文件大小：映射文件最大1G，在消息写入过程中，有可能出现文件不够存储消息的场景，这时候需要创建新的映射文件写入，原文件大小就在1G以内
 *     </li>
 *     <li>
 *         文件名称：文件名称是一串20位的纯数字，文件名称是当前文件的名称+文件当前可写入的位置。
 *         <ol>
 *             1.比如队列中的第一条消息，文件名称为20位0，文件可写入的位置为0，则该消息的queueOffset为0
 *             2.比如队列中的第N条消息，文件名称为20位n，文件可写入的位置位1，则该消息的queueOffset=20位n+1
 *         </ol>
 *     </li>
 *     <li>
 *         文件路径：默认为ENV(user.name)/store/commitlog/00000000000000000000
 *     </li>
 * </ul>
 */
@ImportantPoint("代表存储消息的文件类")
public interface MappedFile {
    /**
     * @return 返回文件名称，其同时作为全局的偏移量
     */
    String getFileName();

    /**
     * 重命名文件名称
     *
     * @param fileName 新的文件名称
     */
    boolean renameTo(String fileName);

    /**
     * @return 返回文件大小
     */
    int getFileSize();

    /**
     * @return 返回基于文件的{@link FileChannel}
     */
    FileChannel getFileChannel();

    /**
     * @return 返回文件是否已经满了，满了就无法再写入消息
     */
    boolean isFull();

    /**
     * @return 返回文件是否可用，如果该文件被停止或者摧毁，代表不可用
     */
    boolean isAvailable();

    /**
     * 向文件末尾追加消息，执行完成后触发回调
     *
     * @param message 要追加的消息
     * @param messageCallback 在执行完消息追加后要执行的回调
     * @param putMessageContext
     * @return 追加结果
     */
    @ImportantPoint("将消息写入到物理文件")
    AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * 向文件末尾追加一批消息，执行完成后触发回调
     *
     * @param message 要追加的消息
     * @param messageCallback 在执行完消息追加后要执行的回调
     * @param putMessageContext
     * @return 追加结果
     */
    AppendMessageResult appendMessages(MessageExtBatch message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * 向文件末尾追加代表消息的字节缓冲，执行完成后触发回调
     *
     * @param byteBufferMsg 包含消息的缓冲区
     * @param cb 压缩的消息回调
     * @return 追加结果
     */
    AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb);

    /**
     * 向文件末尾追加代表消息的字节数组，
     *
     * @param data 包含消息的字节数组
     * @return 追加结果
     */
    boolean appendMessage(byte[] data);


    /**
     * 向文件末尾使用{@link FileChannel}追加代表消息的字节数组，
     *
     * @param data 包含消息的字节数组
     * @return 追加结果
     */
    boolean appendMessageUsingFileChannel(byte[] data);

    /**
     * 向文件末尾追加代表消息的字节缓冲区
     *
     * @param data 包含消息的缓冲区
     * @return 追加结果
     */
    boolean appendMessage(ByteBuffer data);

    /**
     * 向文件末尾追加代表消息的指定区域的字节数组
     *
     * @param data 要追加的数据
     * @param offset 数据在数组开始的索引位置
     * @param length 数据长度
     * @return 追加结果
     */
    boolean appendMessage(byte[] data, int offset, int length);

    /**
     * @return 返回当前文件全局偏移量，即获取其文件名称
     */
    long getFileFromOffset();

    /**
     * 立刻将缓存中的数据刷新到磁盘
     *
     * @param flushLeastPages 要刷新的最少页数
     * @return 刷新后位置
     */
    int flush(int flushLeastPages);

    /**
     * 立刻将二级缓存中的数据刷新到页缓存或磁盘
     *
     * @param commitLeastPages 要提交的最少页数
     * @return 提交后的位置
     */
    int commit(int commitLeastPages);

    /**
     * @param pos 开始位置
     * @param size 大小
     * @return 返回当前文件中指定区域的MappedBuffer
     */
    SelectMappedBufferResult selectMappedBuffer(int pos, int size);

    /**
     * @param pos 开始位置
     * @return 以指定位置为起点，文件末尾作为终点，返回该区域的映射字节缓冲池
     */
    SelectMappedBufferResult selectMappedBuffer(int pos);

    /**
     * @return 返回该文件背后的映射字节缓冲区
     */
    MappedByteBuffer getMappedByteBuffer();

    /**
     * @return 返回该文件背后的字节缓冲区
     */
    ByteBuffer sliceByteBuffer();

    /**
     * @return 返回该文件中最后一条消息的存储时间
     */
    long getStoreTimestamp();

    /**
     * @return 返回该文件最后编辑的时间戳
     */
    long getLastModifiedTimestamp();

    /**
     * 从文件中指定位置获取指定大小的数据，
     * 并将数据保存到ByteBuffer中
     *
     * @param pos 开始位置
     * @param size 数据大小
     * @param byteBuffer 保存获取到的数据
     * @return 获取结果
     */
    boolean getData(int pos, int size, ByteBuffer byteBuffer);

    /**
     * 销毁文件，并从文件系统中删除该文件
     *
     * @param intervalForcibly 如果{@code true}，该方法将强制销毁文件并忽略引用
     * @return 操作结果
     */
    boolean destroy(long intervalForcibly);

    /**
     * 停止文件，并标记为不可用
     *
     * @param intervalForcibly 如果{@code true}，该方法将强制销毁文件并忽略引用
     */
    void shutdown(long intervalForcibly);

    /**
     * 将引用计数-1，并在引用计数到达0时清理映射文件
     */
    void release();

    /**
     * 将引用计数+1
     *
     * @return 操作结果
     */
    boolean hold();

    /**
     * @return {@code true}如果该文件是某个consume queue的第一个映射文件
     */
    boolean isFirstCreateInQueue();

    /**
     * 为当前文件设置标识符，即是否为某个consume queue的第一个文件
     *
     * @param firstCreateInQueue true or false
     */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /**
     * @return 返回该映射文件的刷新位置
     */
    int getFlushedPosition();

    /**
     * 设置该映射文件的已刷新位置
     *
     * @param flushedPosition 已刷新位置
     */
    void setFlushedPosition(int flushedPosition);

    /**
     * @return 返回该映射文件的已写入位置
     */
    int getWrotePosition();

    /**
     * 设置该映射文件的已写入位置
     *
     * @param wrotePosition 写入位置
     */
    void setWrotePosition(int wrotePosition);

    /**
     * @return 返回当前映射文件最大可读位置
     */
    int getReadPosition();

    /**
     * 设置该映射文件的已提交位置
     *
     * @param committedPosition 已提交的位置
     */
    void setCommittedPosition(int committedPosition);

    /**
     * 锁定映射的字节缓冲区
     */
    void mlock();

    /**
     * 解锁映射的字节缓冲区
     */
    void munlock();

    /**
     * 预热映射的字节缓冲区
     *
     * @param type 刷新到磁盘类型
     * @param pages 页
     */
    void warmMappedFile(FlushDiskType type, int pages);

    /**
     * 交换map
     */
    boolean swapMap();

    /**
     * 清理交换的map，即pageTable
     */
    void cleanSwapedMap(boolean force);

    /**
     * 返回最近的交换map时间
     */
    long getRecentSwapMapTime();

    /**
     * 返回从上次交换后，最近访问MappedByteBuffer的次数
     */
    long getMappedByteBufferAccessCountSinceLastSwap();

    /**
     * @return 返回底层文件
     */
    File getFile();

    /**
     * 将文件重命名为带有.delete后缀的名称，
     * 用于之后检测删除
     */
    void renameToDelete();

    /**
     * 将文件移动到父级目录中
     *
     * @throws IOException 文件操作异常
     */
    void moveToParent() throws IOException;

    /**
     * @return 返回最后一次的刷新时间
     */
    long getLastFlushTime();

    /**
     * 初始化映射的文件
     *
     * @param fileName 文件名称
     * @param fileSize 文件大小
     * @param transientStorePool 易变存储池
     * @throws IOException 文件操作异常
     */
    void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException;

    /**
     * @param pos 开始位置
     * @return 从指定位置开始的迭代器
     */
    Iterator<SelectMappedBufferResult> iterator(int pos);

    /**
     * 检查给定的位置和大小的映射文件是否已经加载到内存中
     *
     * @param position 数据的开始偏移量
     * @param size 数据大小
     * @return 加载结果
     */
    boolean isLoaded(long position, int size);
}

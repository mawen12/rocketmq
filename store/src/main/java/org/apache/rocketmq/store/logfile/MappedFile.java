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
     * @param fileName the new file name
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
     * @param message a message to append
     * @param messageCallback the specific call back to execute the real append action
     * @param putMessageContext
     * @return the append result
     */
    @ImportantPoint("将消息写入到物理文件")
    AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * 向文件末尾追加一批消息，执行完成后触发回调
     *
     * @param message a message to append
     * @param messageCallback the specific call back to execute the real append action
     * @param putMessageContext
     * @return the append result
     */
    AppendMessageResult appendMessages(MessageExtBatch message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * 向文件末尾追加代表消息的字节缓冲，执行完成后触发回调
     *
     * @param byteBufferMsg
     * @param cb
     * @return
     */
    AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb);

    /**
     * 向文件末尾追加代表消息的字节数组，
     *
     * @param data the byte array to append
     * @return true if success; false otherwise.
     */
    boolean appendMessage(byte[] data);


    /**
     * 向文件末尾使用{@link FileChannel}追加代表消息的字节数组，
     *
     * @param data the byte array to append
     * @return true if success; false otherwise.
     */
    boolean appendMessageUsingFileChannel(byte[] data);

    /**
     * 向文件末尾追加代表消息的字节缓冲区
     *
     * @param data the byte buffer to append
     * @return true if success; false otherwise.
     */
    boolean appendMessage(ByteBuffer data);

    /**
     * 向文件末尾追加代表消息的指定区域的字节数组
     *
     * @param data the byte array to append
     * @param offset the offset within the array of the first byte to be read
     * @param length the number of bytes to be read from the given array
     * @return true if success; false otherwise.
     */
    boolean appendMessage(byte[] data, int offset, int length);

    /**
     * 返回当前文件全局偏移量，即获取其文件名称
     *
     * @return the offset of this file
     */
    long getFileFromOffset();

    /**
     * 立刻将缓存中的数据刷新到磁盘
     *
     * @param flushLeastPages the least pages to flush
     * @return the flushed position after the method call
     */
    int flush(int flushLeastPages);

    /**
     * 立刻将二级缓存中的数据刷新到页缓存或磁盘
     *
     * @param commitLeastPages the least pages to commit
     * @return the committed position after the method call
     */
    int commit(int commitLeastPages);

    /**
     * @param pos the given position
     * @param size the size of the returned sub-region
     * @return 返回当前文件中指定区域的MappedBuffer
     */
    SelectMappedBufferResult selectMappedBuffer(int pos, int size);

    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos the given position
     * @return
     */
    SelectMappedBufferResult selectMappedBuffer(int pos);

    /**
     * Returns the mapped byte buffer behind the mapped file.
     *
     * @return the mapped byte buffer
     */
    MappedByteBuffer getMappedByteBuffer();

    /**
     * Returns a slice of the mapped byte buffer behind the mapped file.
     *
     * @return the slice of the mapped byte buffer
     */
    ByteBuffer sliceByteBuffer();

    /**
     * Returns the store timestamp of the last message.
     *
     * @return the store timestamp
     */
    long getStoreTimestamp();

    /**
     * Returns the last modified timestamp of the file.
     *
     * @return the last modified timestamp
     */
    long getLastModifiedTimestamp();

    /**
     * Get data from a certain pos offset with size byte
     *
     * @param pos a certain pos offset to get data
     * @param size the size of data
     * @param byteBuffer the data
     * @return true if with data; false if no data;
     */
    boolean getData(int pos, int size, ByteBuffer byteBuffer);

    /**
     * Destroys the file and delete it from the file system.
     *
     * @param intervalForcibly If {@code true} then this method will destroy the file forcibly and ignore the reference
     * @return true if success; false otherwise.
     */
    boolean destroy(long intervalForcibly);

    /**
     * Shutdowns the file and mark it unavailable.
     *
     * @param intervalForcibly If {@code true} then this method will shutdown the file forcibly and ignore the reference
     */
    void shutdown(long intervalForcibly);

    /**
     * Decreases the reference count by {@code 1} and clean up the mapped file if the reference count reaches at
     * {@code 0}.
     */
    void release();

    /**
     * Increases the reference count by {@code 1}.
     *
     * @return true if success; false otherwise.
     */
    boolean hold();

    /**
     * Returns true if the current file is first mapped file of some consume queue.
     *
     * @return true or false
     */
    boolean isFirstCreateInQueue();

    /**
     * Sets the flag whether the current file is first mapped file of some consume queue.
     *
     * @param firstCreateInQueue true or false
     */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /**
     * Returns the flushed position of this mapped file.
     *
     * @return the flushed posotion
     */
    int getFlushedPosition();

    /**
     * Sets the flushed position of this mapped file.
     *
     * @param flushedPosition the specific flushed position
     */
    void setFlushedPosition(int flushedPosition);

    /**
     * Returns the wrote position of this mapped file.
     *
     * @return the wrote position
     */
    int getWrotePosition();

    /**
     * Sets the wrote position of this mapped file.
     *
     * @param wrotePosition the specific wrote position
     */
    void setWrotePosition(int wrotePosition);

    /**
     * Returns the current max readable position of this mapped file.
     *
     * @return the max readable position
     */
    int getReadPosition();

    /**
     * Sets the committed position of this mapped file.
     *
     * @param committedPosition the specific committed position
     */
    void setCommittedPosition(int committedPosition);

    /**
     * Lock the mapped bytebuffer
     */
    void mlock();

    /**
     * Unlock the mapped bytebuffer
     */
    void munlock();

    /**
     * Warm up the mapped bytebuffer
     * @param type
     * @param pages
     */
    void warmMappedFile(FlushDiskType type, int pages);

    /**
     * Swap map
     */
    boolean swapMap();

    /**
     * Clean pageTable
     */
    void cleanSwapedMap(boolean force);

    /**
     * Get recent swap map time
     */
    long getRecentSwapMapTime();

    /**
     * Get recent MappedByteBuffer access count since last swap
     */
    long getMappedByteBufferAccessCountSinceLastSwap();

    /**
     * Get the underlying file
     * @return
     */
    File getFile();

    /**
     * rename file to add ".delete" suffix
     */
    void renameToDelete();

    /**
     * move the file to the parent directory
     * @throws IOException
     */
    void moveToParent() throws IOException;

    /**
     * Get the last flush time
     * @return
     */
    long getLastFlushTime();

    /**
     * Init mapped file
     * @param fileName file name
     * @param fileSize file size
     * @param transientStorePool transient store pool
     * @throws IOException
     */
    void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException;

    Iterator<SelectMappedBufferResult> iterator(int pos);

    /**
     * Check mapped file is loaded to memory with given position and size
     * @param position start offset of data
     * @param size data size
     * @return data is resided in memory or not
     */
    boolean isLoaded(long position, int size);
}

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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 临时存储池
 */
public class TransientStorePool {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 缓存池大小，默认为5
     *
     * @see org.apache.rocketmq.store.config.MessageStoreConfig#transientStorePoolSize
     */
    private final int poolSize;
    /**
     * 文件大小，默认为1G
     *
     * @see org.apache.rocketmq.store.config.MessageStoreConfig#mappedFileSizeCommitLog
     */
    private final int fileSize;
    /**
     * 可用字节缓存的队列，默认为5个1G的文件，该队列中实际保存了消息的数据
     *
     * <p>该直接缓冲底层是直接分配在内存上
     */
    private final Deque<ByteBuffer> availableBuffers;
    /**
     * 是否实时提交，默认为实时提交
     */
    private volatile boolean isRealCommit = true;

    public TransientStorePool(final int poolSize, final int fileSize) {
        this.poolSize = poolSize;
        this.fileSize = fileSize;
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * 这是一个高昂的初始化方法
     */
    public void init() {
        // 分配指定池大小的缓存区，每个缓存区都是一个文件大小
        for (int i = 0; i < poolSize; i++) {
            // 在内存上分配OS可以直接访问的区域
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁方法，需要将之前申请的内存释放掉
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            // 获取内存地址
            final long address = ((DirectBuffer) byteBuffer).address();
            // 构造指向该地址的指针
            Pointer pointer = new Pointer(address);
            // 解锁
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 将内存中的第一个字节缓冲区取出，并保存到参数中
     *
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * @return 返回头部缓存区
     */
    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        return availableBuffers.size();
    }

    public boolean isRealCommit() {
        return isRealCommit;
    }

    public void setRealCommit(boolean realCommit) {
        isRealCommit = realCommit;
    }
}

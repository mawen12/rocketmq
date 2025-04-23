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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * 默认的{@link MappedFile}实现
 */
public class DefaultMappedFile extends AbstractMappedFile {
    /**
     * 操作系统 page cache大小，默认为4k
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    public static final Unsafe UNSAFE = getUnsafe();

    private static final Method IS_LOADED_METHOD;

    public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 总的映射虚拟内存大小
     */
    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 总的映射文件数
     */
    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 写入位置的原子引用，指向{@link #wrotePosition}
     */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;

    /**
     * 已提交位置的原子引用，指向{@link #committedPosition}
     */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;

    /**
     * 已刷新位置的原子引用，指向{@link #flushedPosition}
     */
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");

        Method isLoaded0method = null;
        // On the windows platform and openjdk 11 method isLoaded0 always returns false.
        // see https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/java.base/windows/native/libnio/MappedByteBuffer.c#L34
        if (!SystemUtils.IS_OS_WINDOWS) {
            try {
                isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
                isLoaded0method.setAccessible(true);
            } catch (NoSuchMethodException ignore) {
            }
        }
        IS_LOADED_METHOD = isLoaded0method;
    }

    /**
     * 文件已写入位置，当该值与{@link #fileSize}相等时，代表文件已经满了
     */
    protected volatile int wrotePosition;
    /**
     * 文件已提交位置
     */
    protected volatile int committedPosition;
    /**
     * 文件已刷新位置
     */
    protected volatile int flushedPosition;
    /**
     * 文件大小
     */
    protected int fileSize;
    /**
     * 基于{@link #file}的文件通道
     */
    protected FileChannel fileChannel;
    /**
     * 对于用户发送请求的消息，首先会放在此处，如果该值不为空，则将其放到{@link #fileChannel}中
     */
    protected ByteBuffer writeBuffer = null;
    /**
     * 临时缓存池
     */
    protected TransientStorePool transientStorePool = null;
    /**
     * 文件名称
     */
    protected String fileName;
    /**
     * 文件起始偏移量
     */
    protected long fileFromOffset;
    /**
     * 底层的原始文件
     */
    protected File file;

    protected MappedByteBuffer mappedByteBuffer;
    /**
     * 最新一条消息存储的时间戳
     */
    protected volatile long storeTimestamp = 0;
    /**
     * 是否为队列中的第一个文件的标识
     */
    protected boolean firstCreateInQueue = false;
    /**
     * 最后一条消息的刷新时间
     */
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    /**
     *  如果该文件属于consume queue，该字段存储首个被逻辑队列引用的首条消息的存储时间
     */
    private long startTimestamp = -1;

    /**
     * 如果该文件属于consume queue，该字段存储首个被逻辑队列引用的最后一条消息的存储时间
     */
    private long stopTimestamp = -1;

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    @Override
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        // 文件名称作为起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 检查目录存在
        UtilAll.ensureDirOK(this.file.getParent());

        try {
            // 读写模式的文件信道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 读写模式的字节缓冲区
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 累加文件大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            // 累加文件数
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public boolean renameTo(String fileName) {
        File newFile = new File(fileName);
        boolean rename = file.renameTo(newFile);
        if (rename) {
            this.fileName = fileName;
            this.file = newFile;
        }
        return rename;
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        // 如果buffer可用空间不足以容纳那么多数据，便直接返回
        if (byteBuffer.remaining() < size) {
            return false;
        }

        //
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb) {
        assert byteBufferMsg != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(byteBuffer, this.fileFromOffset, this.fileSize - currentPos, byteBufferMsg);
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 将消息写入到物理文件上
     *
     * @param msg a message to append
     * @param cb the specific call back to execute the real append action
     * @param putMessageContext
     * @return
     */
    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
        PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;

        // 获取当前文件写入位置
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        // 检查文件位置是否小于文件大小，超过代表无法写入
        if (currentPos < this.fileSize) {
            // 获取保存消息的字节缓冲区
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            // 定位到可写位置
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                // 传统批次消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                // 支持传统的单个消息或新引入到的内部批次消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新写入位置
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            // 更新最新的消息存储时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remaining = data.remaining();

        if ((currentPos + remaining) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                while (data.hasRemaining()) {
                    this.fileChannel.write(data);
                }
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, remaining);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        // 获取写的位置
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if ((currentPos + length) <= this.fileSize) {// 检查当前文件能否写入这么多消息。因为文件限制了最大1G
            try {
                // 获取一个用于单独惭怍的缓冲区
                ByteBuffer buf = this.mappedByteBuffer.slice();
                // 更新为之前写的位置
                buf.position(currentPos);
                // 写入消息
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新写的位置=之前位置+本地写入长度
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }

        // 写入的内容超过了文件的限制，无法写入，返回false
        return false;
    }

    @Override
    public boolean appendMessageUsingFileChannel(byte[] data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, 0, data.length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, data.length);
            return true;
        }

        return false;
    }

    /**
     * @return 当前已刷新的位置
     */
    @Override
    public int flush(final int flushLeastPages) {
        // 检查是否可以刷新
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                // 获取读的位置
                int value = getReadPosition();

                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;

                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                FLUSHED_POSITION_UPDATER.set(this, value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    @Override
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return WROTE_POSITION_UPDATER.get(this);
        }

        //no need to commit data to file channel, so just set committedPosition to wrotePosition.
        if (transientStorePool != null && !transientStorePool.isRealCommit()) {
            COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
        } else if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return COMMITTED_POSITION_UPDATER.get(this);
    }

    protected void commit0() {
        int writePos = WROTE_POSITION_UPDATER.get(this);
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);

        if (writePos - lastCommittedPosition > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                COMMITTED_POSITION_UPDATER.set(this, writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();

        // 如果文件满了，代表可以刷新
        if (this.isFull()) {
            return true;
        }

        // 如果强制刷新页数，并且最大合法位置/系统页 - 刷新位置/系统页 >= 强制刷新页数，代表可以刷新
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // 如果最大合法位置超过了刷新位置，代表可以刷新
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        int write = WROTE_POSITION_UPDATER.get(this);

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    /**
     * 检查文件是否已经满了，当满了之后就不可写入了
     *
     * @return
     */
    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;

                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime)
                    + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * 如果未使用临时存储池，或者临时存储池不是实时提交的，则从{@link #WROTE_POSITION_UPDATER}获取。
     * 如果使用了临时存储池且开启了实时提交，则从{@link #COMMITTED_POSITION_UPDATER}获取
     *
     * @return 存有合法数据的最大可读位置
     */
    @Override
    public int getReadPosition() {
        return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long flush = 0;
        // long time = System.currentTimeMillis();
        for (long i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put((int) i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // if (j % 1000 == 0) {
            //     log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
            //     time = System.currentTimeMillis();
            //     try {
            //         Thread.sleep(0);
            //     } catch (InterruptedException e) {
            //         log.error("Interrupted", e);
            //     }
            // }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return false;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
                return true;
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public void renameToDelete() {
        //use Files.move
        if (!fileName.endsWith(".delete")) {
            String newFileName = this.fileName + ".delete";
            try {
                Path newFilePath = Paths.get(newFileName);
                // https://bugs.openjdk.org/browse/JDK-4724038
                // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
                // Windows can't move the file when mmapped.
                if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
                    long position = this.fileChannel.position();
                    UtilAll.cleanBuffer(this.mappedByteBuffer);
                    this.fileChannel.close();
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                    try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
                        this.fileChannel = file.getChannel();
                        this.fileChannel.position(position);
                        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    }
                } else {
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                }
                this.fileName = newFileName;
                this.file = new File(newFileName);
            } catch (IOException e) {
                log.error("move file {} failed", fileName, e);
            }
        }
    }

    @Override
    public void moveToParent() throws IOException {
        Path currentPath = Paths.get(fileName);
        String baseName = currentPath.getFileName().toString();
        Path parentPath = currentPath.getParent().getParent().resolve(baseName);
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmapped.
        if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
            long position = this.fileChannel.position();
            UtilAll.cleanBuffer(this.mappedByteBuffer);
            this.fileChannel.close();
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
            try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
                this.fileChannel = file.getChannel();
                this.fileChannel.position(position);
                this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            }
        } else {
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
        }
        this.file = parentPath.toFile();
        this.fileName = parentPath.toString();
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }

    public Iterator<SelectMappedBufferResult> iterator(int startPos) {
        return new Itr(startPos);
    }

    public static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static long mappingAddr(long addr) {
        long offset = addr % UNSAFE_PAGE_SIZE;
        offset = (offset >= 0) ? offset : (UNSAFE_PAGE_SIZE + offset);
        return addr - offset;
    }

    public static int pageCount(long size) {
        return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
    }

    @Override
    public boolean isLoaded(long position, int size) {
        if (IS_LOADED_METHOD == null) {
            return true;
        }
        try {
            long addr = ((DirectBuffer) mappedByteBuffer).address() + position;
            return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
        } catch (Exception e) {
            log.info("invoke isLoaded0 of file {} error:", file.getAbsolutePath(), e);
        }
        return true;
    }

    private class Itr implements Iterator<SelectMappedBufferResult> {
        private int start;
        private int current;
        private ByteBuffer buf;

        public Itr(int pos) {
            this.start = pos;
            this.current = pos;
            this.buf = mappedByteBuffer.slice();
            this.buf.position(start);
        }

        @Override
        public boolean hasNext() {
            return current < getReadPosition();
        }

        @Override
        public SelectMappedBufferResult next() {
            int readPosition = getReadPosition();
            if (current < readPosition && current >= 0) {
                if (hold()) {
                    ByteBuffer byteBuffer = buf.slice();
                    byteBuffer.position(current);
                    int size = byteBuffer.getInt(current);
                    ByteBuffer bufferResult = byteBuffer.slice();
                    bufferResult.limit(size);
                    current += size;
                    return new SelectMappedBufferResult(fileFromOffset + current, bufferResult, size,
                        DefaultMappedFile.this);
                }
            }
            return null;
        }

        @Override
        public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
            Iterator.super.forEachRemaining(action);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

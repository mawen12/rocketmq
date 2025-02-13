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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * 默认的{@link HAClient}实现
 *
 * <p>仅当该Broker为Slave时才会被创建，创建逻辑位于{@link DefaultHAService#init(DefaultMessageStore)}
 *
 * <p>该客户端启动后作为一个服务线程
 */
public class DefaultHAClient extends ServiceThread implements HAClient {

    /**
     * 上报头的缓存大小。Schema：slaveMaxOffset，格式为：
     * <pre>
     * ┌───────────────────────────────────────────────┐
     * │                  slaveMaxOffset               │
     * │                    (8bytes)                   │
     * ├───────────────────────────────────────────────┤
     * │                                               │
     * │                  Report Header                │
     * </pre>
     * <p>
     */
    public static final int REPORT_HEADER_SIZE = 8;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 读取的最大缓存为4M
     */
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    /**
     * 可以被原子更新的 master high available地址，该变量是多线程可见的
     */
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    /**
     * 可以被原子更新的 master 地址，该变量是多线程可见的
     */
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    /**
     * 上报头缓冲区，8字节
     */
    private final ByteBuffer reportOffset = ByteBuffer.allocate(REPORT_HEADER_SIZE);
    /**
     * nio的Socket信道
     */
    private SocketChannel socketChannel;
    /**
     * nio的选择器
     */
    private Selector selector;
    /**
     * salve从master读取日期的最后时间戳
     */
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * slave上报offset到master的最后时间戳
     */
    private long lastWriteTimestamp = System.currentTimeMillis();
    /**
     * 当前上报的offset
     */
    private long currentReportedOffset = 0;

    private int dispatchPosition = 0;
    /**
     * 读缓冲区
     */
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    /**
     * 备份缓冲区
     */
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    /**
     * 消息存储，用于将master传递来的消息保存到本地
     */
    private DefaultMessageStore defaultMessageStore;
    /**
     * 异变的连接状态，该变量是多线程可见的，初始状态为{@link HAConnectionState#READY}，代表可以连接
     *
     * <p>因为该线程的关闭是可能被其管理者线程触发的，这是一种典型的多线程场景
     */
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    /**
     * 用于监听传输字节数的工具
     */
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        this.selector = NetworkUtil.openSelector();
        this.defaultMessageStore = defaultMessageStore;
        this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
    }

    public void updateHaMasterAddress(final String newAddr) {
        String currentAddr = this.masterHaAddress.get();
        if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public void updateMasterAddress(final String newAddr) {
        String currentAddr = this.masterAddress.get();
        if (masterAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    /**
     * @see DefaultMessageStore#now()
     * @see #lastWriteTimestamp
     * @see MessageStoreConfig#getHaSendHeartbeatInterval()
     *
     * @return 是否到应该上报的时间，通过当前时间-上次写入的时间获取时间差，是否超过配置的心跳值，默认为5s
     */
    private boolean isTimeToReportOffset() {
        long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
        return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    /**
     * slave上报最大可读偏移量给master
     *
     * @param maxOffset 要上报的偏移量
     * @return
     */
    private boolean reportSlaveMaxOffset(final long maxOffset) {
        // 设置起点为0
        this.reportOffset.position(0);
        // 设置终点为8，即仅接受8字节的内容
        this.reportOffset.limit(REPORT_HEADER_SIZE);
        // 写入要上报的偏移量
        this.reportOffset.putLong(maxOffset);
        // TODO by mawen 为什么需要重置？
        // 回到起点
        this.reportOffset.position(0);
        // 设置终点为8，截掉
        this.reportOffset.limit(REPORT_HEADER_SIZE);

        // 次数小于3且仅当本地传输未完全读取时才会重试
        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                // 发起通知
                this.socketChannel.write(this.reportOffset);
            } catch (IOException e) {
                log.error(this.getServiceName()
                    + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                return false;
            }
        }
        lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
        // 检查上报还有剩余的话，代表网络传输出现了问题，返回false；否则返回true
        return !this.reportOffset.hasRemaining();
    }

    /**
     * 重分配缓冲区
     */
    private void reallocateByteBuffer() {
        // 获取保留空间
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        if (remain > 0) {
            this.byteBufferRead.position(this.dispatchPosition);

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            this.byteBufferBackup.put(this.byteBufferRead);
        }

        this.swapByteBuffer();

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    /**
     * 处理读事件，即从信道中读取从master传递过来的消息，并将其保存到本地磁盘上
     *
     * @return
     */
    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        // 如果读缓冲区还有元素，则一直循环下去，确保将读取操作执行完
        while (this.byteBufferRead.hasRemaining()) {
            try {
                // 将网络读取到该缓冲区
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {// 读取到数据
                    // 写入流控
                    flowMonitor.addByteCountTransferred(readSize);
                    // 因为本地读取到了数据，重置读取为0的次数
                    readSizeZeroTimes = 0;
                    // 将读取到的消息保存到磁盘中
                    boolean result = this.dispatchReadRequest();
                    if (!result) {
                        log.error("HAClient, dispatchReadRequest error");
                        // 处理失败，直接返回
                        return false;
                    }
                    // 更新最后读取成功的时间戳
                    lastReadTimestamp = System.currentTimeMillis();
                } else if (readSize == 0) {// 未读取到数据
                    if (++readSizeZeroTimes >= 3) {// 将读取到0的次数加1，并检查是否超过3次
                        // 连接三次读取到0时，代表master从上次读到本地读之间，并未有新的消息保存，便跳出循环
                        break;
                    }
                } else {
                    // 处理读取到数据<0的场景
                    log.info("HAClient, processReadEvent read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                // 处理读取数据异常的场景
                log.info("HAClient, processReadEvent read socket exception", e);
                return false;
            }
        }

        // 执行成功，返回
        return true;
    }

    private boolean dispatchReadRequest() {
        // 获取上次处理的位置
        int readSocketPos = this.byteBufferRead.position();

        while (true) {
            // 计算本次读取的偏移量和 差值
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            if (diff >= DefaultHAConnection.TRANSFER_HEADER_SIZE) {// 如果差值超过了传输头，代表此数据尚未处理
                // 从上次处理位置获取 masterPhyOffset，占8字节
                long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                // 从masterPhyOffset之后读取bodySize，占4字节
                int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);
                // 获取本地存储的最大可读位置
                long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();

                if (slavePhyOffset != 0) {
                    // 当slave和master的phyOffset不一致时，此时无法处理，因为消息复制是渐进的
                    if (slavePhyOffset != masterPhyOffset) {
                        log.error("master pushed offset not equal the max phy offset in slave, SLAVE: " + slavePhyOffset + " MASTER: " + masterPhyOffset);
                        return false;
                    }
                }

                if (diff >= (DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize)) {// 代表传递了消息内容
                    // 获取消息体
                    byte[] bodyData = byteBufferRead.array();
                    // 计算消息起始位置，即起始位置+传输头
                    int dataStart = this.dispatchPosition + DefaultHAConnection.TRANSFER_HEADER_SIZE;
                    // 将消息内容以追加形式保存到磁盘的commitlog上
                    this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData, dataStart, bodySize);
                    // 更新读位置为上次的读取位置
                    this.byteBufferRead.position(readSocketPos);
                    // 更新起始位置=之前位置+传输头大小+消息体大小
                    this.dispatchPosition += DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize;

                    // 将本次更新的偏移量上报给master，因为之前从master获取到消息，并保存到本地，因此偏移量肯定是增长了的
                    // 上报失败，会导致关闭与master的连接
                    if (!reportSlaveMaxOffsetPlus()) {
                        return false;
                    }

                    // 结束本次处理，进入下一次循环
                    continue;
                }
            }

            if (!this.byteBufferRead.hasRemaining()) {
                // 执行重分配
                this.reallocateByteBuffer();
            }

            // 执行到此处，代表消息已经保存完成，且成功通知到master，本地的缓冲区也进行了更新，便跳出循环
            break;
        }

        // 执行成功
        return true;
    }

    /**
     * 将{@link #currentReportedOffset}上报到master，如果上报失败，则关闭与master的连接
     *
     * @return true 上报成功；false 上报失败
     */
    private boolean reportSlaveMaxOffsetPlus() {
        boolean result = true;
        // 获取本地存储中最大的可读位置
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        if (currentPhyOffset > this.currentReportedOffset) {// 仅在本次的位置大于上次的上报位置时，才会触发上报
            // 更新上报位置
            this.currentReportedOffset = currentPhyOffset;
            // 执行上报
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {// 上报失败时，代表master出现了问题，需要关闭与master的连接，并打印错误日志
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        // 上报成功，或者之前获取到的消息
        return result;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    /**
     * 连接到master high available地址
     *
     * <p>连接失败的可能
     * <ul>
     *     <li>{@link #masterHaAddress}地址为空</li>
     *     <li>{@link #masterHaAddress}地址错误</li>
     * </ul>
     *
     * @return true 连接成功
     * @throws ClosedChannelException
     */
    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            String addr = this.masterHaAddress.get();
            if (addr != null) {
                // 将地址转换为SocketAddress
                SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
                // 创建连接
                this.socketChannel = RemotingHelper.connect(socketAddress);
                if (this.socketChannel != null) {
                    // 注册读操作的选择器
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    log.info("HAClient connect to master {}", addr);
                    // 更新状态为可同步数据
                    this.changeCurrentState(HAConnectionState.TRANSFER);
                }
            }

            // 获取本地当前最大物理偏移量，作为上报的偏移量
            this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();
            // 更新最后读取时间
            this.lastReadTimestamp = System.currentTimeMillis();
        }

        return this.socketChannel != null;
    }

    /**
     * 关闭与master high available的连接
     */
    public void closeMaster() {
        if (null != this.socketChannel) {
            try {
                // 获取该选择器的token
                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    // 执行取消，放入到{@code selector#cancelled-key set}中
                    sk.cancel();
                }

                // 关闭信道
                this.socketChannel.close();

                // 置空
                this.socketChannel = null;

                log.info("HAClient close connection with master {}", this.masterHaAddress.get());
                // 更新状态为可连接状态
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                log.warn("closeMaster exception. ", e);
            }

            // 均更新为初始值
            this.lastReadTimestamp = 0;
            this.dispatchPosition = 0;

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        // 当该客户端启动时，同时开启流控
        this.flowMonitor.start();

        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        // 当信道暂停时，同时停止流控
                        this.flowMonitor.shutdown(true);
                        // 因为已经停止，跳出循环
                        return;
                    case READY:
                        if (!this.connectMaster()) {
                            // 连接master high available地址失败，日志提示错误，
                            log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
                            // 等待5s后重试
                            this.waitForRunning(1000 * 5);
                        }
                        // 结束本次循环
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    default:
                        this.waitForRunning(1000 * 2);
                        continue;
                }
                long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
                if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    this.closeMaster();
                    log.warn("AutoRecoverHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.closeMasterAndWait();
            }
        }

        this.flowMonitor.shutdown(true);
        log.info(this.getServiceName() + " service end");
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        // 如果已经到上报slave的offset的时间，就执行上报，上报失败，直接返回false
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }

        // 等待1s
        this.selector.select(1000);

        // 如果读取到消息，则保存到磁盘中
        result = this.processReadEvent();
        if (!result) {
            return false;
        }

        // 再次尝试将最大可读位置上报到master
        return reportSlaveMaxOffsetPlus();
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        // 更新状态为暂停
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            log.warn("Close the selector of AutoRecoverHAClient error, ", e);
        }
    }

    @Override
    public String getServiceName() {
        if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return this.defaultMessageStore.getBrokerIdentity().getIdentifier() + DefaultHAClient.class.getSimpleName();
        }
        return DefaultHAClient.class.getSimpleName();
    }
}

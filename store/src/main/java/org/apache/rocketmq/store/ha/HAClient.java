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

/**
 * High Available 客户端
 *
 * <p>用于Master-Slave之间进行消息复制，复制策略有：
 * <ul>
 *     <li>同步复制：{@link org.apache.rocketmq.store.config.BrokerRole#SYNC_MASTER}</li>
 *     <li>异步复制：{@link org.apache.rocketmq.store.config.BrokerRole#ASYNC_MASTER}</li>
 * </ul>
 *
 * @see org.apache.rocketmq.store.config.BrokerRole
 */
public interface HAClient {

    /**
     * 启动客户端
     */
    void start();

    /**
     * 停止客户端
     */
    void shutdown();

    /**
     * 唤醒客户端
     */
    void wakeup();

    /**
     * 更新master地址
     *
     * <p>仅当该Broker为{@link org.apache.rocketmq.store.config.BrokerRole#SLAVE}时才会触发
     *
     * @param newAddress
     */
    void updateMasterAddress(String newAddress);

    /**
     * 更新master high available地址
     *
     * <p>仅当该Broker为}{@link org.apache.rocketmq.store.config.BrokerRole#SLAVE}时才会出发
     *
     * @param newAddress
     */
    void updateHaMasterAddress(String newAddress);

    /**
     * 获取master地址
     *
     * @return master address
     */
    String getMasterAddress();

    /**
     * 获取master high available地址
     *
     * @return master ha address
     */
    String getHaMasterAddress();

    /**
     * 获取该客户端最后读取的时间戳
     *
     * @return last read timestamp
     */
    long getLastReadTimestamp();

    /**
     * 获取该客户端最后写的时间戳
     *
     * @return last write timestamp
     */
    long getLastWriteTimestamp();

    /**
     * 获取链接的当前状态
     *
     * @return HAConnectionState
     */
    HAConnectionState getCurrentState();

    /**
     * 处于测试目的而变更high available链接的当前状态
     *
     * @param haConnectionState
     */
    void changeCurrentState(HAConnectionState haConnectionState);

    /**
     * 处于测试目的断开与master的连接
     */
    void closeMaster();

    /**
     * 获取每秒传输的字节数
     *
     * <p>可用于得知master-slave之间消息复制的快慢
     *
     *  @return transfer bytes in second
     */
    long getTransferredByteInSecond();
}

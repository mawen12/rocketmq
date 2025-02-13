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

import java.nio.channels.SocketChannel;

/**
 * High available 连接
 *
 */
public interface HAConnection {
    /**
     * 开启High available连接
     */
    void start();

    /**
     * 停止High available连接
     */
    void shutdown();

    /**
     * 关闭High available 连接
     */
    void close();

    /**
     * 获取Socket信道
     */
    SocketChannel getSocketChannel();

    /**
     * 获取该连接的当前状态
     *
     * @return HAConnectionState
     */
    HAConnectionState getCurrentState();

    /**
     * 获取该连接的客户端地址
     *
     * @return client ip address
     */
    String getClientAddress();

    /**
     * 获取该连接每秒传输速率
     *
     *  @return transfer bytes in second
     */
    long getTransferredByteInSecond();

    /**
     * 获取当前到slave的传输offset
     *
     * @return the current transfer offset to the slave
     */
    long getTransferFromWhere();

    /**
     * 获取slave ack offset
     *
     * @return slave ack offset
     */
    long getSlaveAckOffset();
}

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
package org.apache.rocketmq.client.producer;

/**
 * 消息发送状态，除了成功状态外，其他都是消息存储失败
 */
public enum SendStatus {
    /**
     * 发送成功
     *
     * <p>即使成功也不代表它是可靠的，要确保不丢失任何消息，还应启动SYNC_MASTER或SYNC_FLUSH
     *
     * @see org.apache.rocketmq.remoting.protocol.ResponseCode#SUCCESS
     */
    SEND_OK,
    /**
     * 发送成功，但是服务器刷盘超时
     *
     * <p>消息已经进入服务器队列（内存），只有服务器宕机，消息才会丢失。消息存储配置参数中可以设置刷盘方式和同步刷盘时间长度。
     * 如果Broker服务器设置了刷盘方式为同步刷盘，即FlushDiskType=SYNC_FLUSH（默认为ASYNC_FLUSH），当Broker服务器未在同步
     * 刷盘时间内（默认为5s）完成刷盘，则将返回该状态--刷盘超时
     *
     * @see org.apache.rocketmq.remoting.protocol.ResponseCode#FLUSH_DISK_TIMEOUT
     */
    FLUSH_DISK_TIMEOUT,
    /**
     * 发送成功，但是服务器同步到Slave时超时
     *
     * <p>消息已经进入服务器队列（内存），只有服务器宕机，消息才会丢失。如果Broker服务器的角色是SYNC_MASTER（默认是ASYNC_MASTER），
     * 并且从Broker服务器未在同步刷盘时间（默认为5s）内完成与主服务器的同步，则将返回该状态--数据同步到Slave服务器超时。
     *
     * @see org.apache.rocketmq.remoting.protocol.ResponseCode#FLUSH_SLAVE_TIMEOUT
     */
    FLUSH_SLAVE_TIMEOUT,
    /**
     * 消息发送成功，但是此时Slave不可用
     *
     * <p>消息已经进入服务器队列（内存），只有服务器宕机，消息才会丢失。如果Broker服务器的角色是SYNC_MASTER（默认是ASYNC_MASTER）,
     * 但没有配置Slave Broker服务器，则将返回该状态--无Slave服务器可用。
     *
     * @see org.apache.rocketmq.remoting.protocol.ResponseCode#SLAVE_NOT_AVAILABLE
     */
    SLAVE_NOT_AVAILABLE,
}

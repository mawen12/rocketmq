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
package org.apache.rocketmq.client.impl;

/**
 * 消息发送模式，默认为同步
 */
public enum CommunicationMode {
    /**
     * 同步，并等待Broker响应，返回{@link org.apache.rocketmq.client.producer.SendResult}
     */
    SYNC,
    /**
     * 异步，提供{@link org.apache.rocketmq.client.producer.SendCallback}来监听Broker响应
     */
    ASYNC,
    /**
     * 单向发送
     *
     * <p>发送过程为：
     * <ul>
     *     <li>客户端发送请求到服务器</li>
     *     <li>服务器处理请求</li>
     *     <li>客户端返回，不等待应答</li>
     * </ul>
     *
     * <p>使用场景，耗时非常短，但是对可靠性要求并不高
     * <ul>
     *     <li>日志收集类应用</li>
     * </ul>
     *
     * <p>实现原理，发送请求在客户端层面仅仅是一个操作系统调用的开销，即将数据写入客户端的socket缓冲区，
     * 此过程耗时通常在微秒级。
     */
    ONEWAY,
}

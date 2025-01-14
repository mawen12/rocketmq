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
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 代表远程服务端
 */
public interface RemotingServer extends RemotingService {
    /**
     * 注册处理器
     *
     * @param requestCode 请求码
     * @param processor 处理器
     * @param executor 处理请求的执行器
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 注册默认的处理器
     *
     * @param processor 处理器
     * @param executor 默认请求的执行器
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * @return 返回本地监听端口
     */
    int localListenPort();

    /**
     * @param requestCode 请求码
     * @return 返回{@link #registerProcessor(int, NettyRequestProcessor, ExecutorService)}一起注册的处理器和执行器
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * @return 返回 {@link #registerDefaultProcessor(NettyRequestProcessor, ExecutorService)}一起注册的处理器和执行器
     */
    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    /**
     * @param port 端口
     * @return 使用指定端口创建远程服务端
     */
    RemotingServer newRemotingServer(int port);

    /**
     * 移除远程服务端
     *
     * @param port 端口
     */
    void removeRemotingServer(int port);

    /**
     * 发起同步调用
     *
     * @param channel 频道
     * @param request 请求
     * @param timeoutMillis 超时时间
     * @return 响应
     * @throws InterruptedException 线程被打断异常
     * @throws RemotingSendRequestException 发送请求异常
     * @throws RemotingTimeoutException 超过了指定超时时间异常
     * @see org.apache.rocketmq.client.impl.CommunicationMode#SYNC
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 发起异步调用
     *
     * @param channel 频道
     * @param request 请求
     * @param timeoutMillis 超时时间
     * @param invokeCallback 响应回调
     * @throws InterruptedException 线程被打断异常
     * @throws RemotingTooMuchRequestException 请求太多异常
     * @throws RemotingTimeoutException 超过了指定超时时间异常
     * @throws RemotingSendRequestException 发送请求异常
     * @see org.apache.rocketmq.client.impl.CommunicationMode#ASYNC
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 发起单向调用，即不必关系响应结果
     *
     * @param channel 频道
     * @param request 请求
     * @param timeoutMillis 超时时间
     * @throws InterruptedException 线程被打断异常
     * @throws RemotingTooMuchRequestException 请求太多异常
     * @throws RemotingTimeoutException 超过了指定超时时间异常
     * @throws RemotingSendRequestException 发送请求异常
     * @see org.apache.rocketmq.client.impl.CommunicationMode#ONEWAY
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

}

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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 代表远程客户端
 */
public interface RemotingClient extends RemotingService {

    void updateNameServerAddressList(final List<String> addrs);

    /**
     * @return 返回所有的Namesrv地址列表
     */
    List<String> getNameServerAddressList();

    /**
     * @return 返回所有可用的Namesrv地址列表
     */
    List<String> getAvailableNameSrvList();

    /**
     * 发起同步调用
     *
     * @param addr          地址
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @return 响应
     * @throws InterruptedException         线程被打断异常
     * @throws RemotingConnectException     连接异常
     * @throws RemotingSendRequestException 发送请求异常
     * @throws RemotingTimeoutException     超过了指定超时时间异常
     * @see org.apache.rocketmq.client.impl.CommunicationMode#SYNC
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 发起异步调用
     *
     * @param addr           地址
     * @param request        请求
     * @param timeoutMillis  超时时间
     * @param invokeCallback 响应回调
     * @throws InterruptedException            线程被打断异常
     * @throws RemotingConnectException        连接异常
     * @throws RemotingSendRequestException    发送请求异常
     * @throws RemotingTooMuchRequestException 请求太多异常
     * @throws RemotingTimeoutException        超过了指定超时时间异常
     * @see org.apache.rocketmq.client.impl.CommunicationMode#ASYNC
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 发起单向调用，即不关心响应结果
     *
     * @param addr          地址，
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @throws InterruptedException            线程被打断异常
     * @throws RemotingConnectException        连接异常
     * @throws RemotingSendRequestException    发送请求异常
     * @throws RemotingTooMuchRequestException 请求太多异常
     * @throws RemotingTimeoutException        超过了指定超时时间异常
     * @see org.apache.rocketmq.client.impl.CommunicationMode#ONEWAY
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 发起请求，底层使用{@link #invokeAsync(String, RemotingCommand, long, InvokeCallback)}来触发调用
     *
     * @param addr          地址
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @return 异步的结果
     */
    default CompletableFuture<RemotingCommand> invoke(final String addr, final RemotingCommand request, final long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {

                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    // 处理请求成功
                    future.complete(response);
                }

                @Override
                public void operationFail(Throwable throwable) {
                    // 处理请求失败
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            // 处理请求异常
            future.completeExceptionally(t);
        }
        return future;
    }

    /**
     * 注册处理器
     *
     * @param requestCode 请求码
     * @param processor   请求处理器
     * @param executor    处理请求的执行器
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 设置回调执行器
     *
     * @param callbackExecutor 回调执行器
     */
    void setCallbackExecutor(final ExecutorService callbackExecutor);

    /**
     * 频道是否可读
     *
     * @param addr 地址
     * @return 可读返回{@code true}
     */
    boolean isChannelWritable(final String addr);

    /**
     * 地址是否可达
     *
     * @param addr 地址
     * @return 地址可达返回{@code true}
     */
    boolean isAddressReachable(final String addr);

    /**
     * 关闭多个频道
     *
     * @param addrList 地址列表
     */
    void closeChannels(final List<String> addrList);
}

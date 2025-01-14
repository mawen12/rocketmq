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
 * 异步发送消息回调，对{@link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer}中提供该参数时，代表发送模式为{@link org.apache.rocketmq.client.impl.CommunicationMode#ASYNC}。
 */
public interface SendCallback {
    /**
     * 通知客户端发送成功，并返回Broker发送结果
     *
     * @param sendResult
     */
    void onSuccess(final SendResult sendResult);

    /**
     * 通知客户端发送异常，并返回异常信息
     * @param e
     */
    void onException(final Throwable e);
}

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

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 将内存内容刷新到磁盘文件
 */
public interface FlushManager {

    /**
     * 开启刷新管理器
     */
    void start();

    /**
     * 关闭刷新管理器
     */
    void shutdown();

    /**
     * 唤醒刷新
     */
    void wakeUpFlush();

    /**
     * 唤醒提交
     */
    void wakeUpCommit();

    /**
     * 处理磁盘刷新
     *
     * @param result
     * @param putMessageResult
     * @param messageExt
     */
    void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt);

    /**
     * 处理磁盘刷新，并返回结果
     *
     * @param result
     * @param messageExt
     * @return
     */
    CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt);
}

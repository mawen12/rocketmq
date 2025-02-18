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
package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.store.Swappable;

/**
 * 基于File系统直接实现的包含consumequeue相关生命周期方法的接口
 */
public interface FileQueueLifeCycle extends Swappable {
    /**
     * 从文件加载
     *
     * @return {@code true}如果加载成功
     */
    boolean load();

    /**
     * 从文件恢复
     */
    void recover();

    /**
     * 检查文件
     */
    void checkSelf();

    /**
     * 将page cache刷新到文件
     *
     * @param flushLeastPages  被刷新的最少page cache数量
     * @return {@code true}刷新成功
     */
    boolean flush(int flushLeastPages);

    /**
     * 摧毁文件
     */
    void destroy();

    /**
     * 从最大commitlog的物理偏移量截断脏逻辑文件
     *
     * @param maxCommitLogPos 最大commitlog位置
     */
    void truncateDirtyLogicFiles(long maxCommitLogPos);

    /**
     * 在最小commitlog的物理偏移量删除过期的文件
     *
     * @param minCommitLogPos 最小commitlog位置
     * @return 删除的文件数量
     */
    int deleteExpiredFile(long minCommitLogPos);

    /**
     * 滚动到文件
     *
     * @param nextBeginOffset 下一个开始的偏移量
     * @return 下一个文件开始的偏移量
     */
    long rollNextFile(final long nextBeginOffset);

    /**
     * 第一个文件是否可用
     *
     * @return {@code true}第一个文件可用
     */
    boolean isFirstFileAvailable();

    /**
     * 第一个文件是否存在
     *
     * @return {@code true}第一个文件存在
     */
    boolean isFirstFileExist();
}

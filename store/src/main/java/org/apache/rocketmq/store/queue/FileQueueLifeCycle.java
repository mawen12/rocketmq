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
 * 包含由 FILE 直接实现的 ConsumeQueue 的生命周期方法
 */
public interface FileQueueLifeCycle extends Swappable {
    /**
     * 从文件中加载内容
     *
     * @return 加载成功返回true
     */
    boolean load();

    /**
     * 从文件中恢复
     */
    void recover();

    /**
     * 检查文件
     */
    void checkSelf();

    /**
     * 将缓存刷新到文件
     *
     * @param flushLeastPages  被刷新的最小页数
     * @return 有数据被刷新时返回true
     */
    boolean flush(int flushLeastPages);

    /**
     * 销毁文件
     */
    void destroy();

    /**
     * 截断最大提交日志位置作为起点的脏逻辑文件
     *
     * @param maxCommitLogPos 最大提交日志位置
     */
    void truncateDirtyLogicFiles(long maxCommitLogPos);

    /**
     * 删除最小提交日志位置的过期文件
     *
     * @param minCommitLogPos 最小提交日志位置
     * @return 删除的文件数量
     */
    int deleteExpiredFile(long minCommitLogPos);

    /**
     * 滚动到下一个文件
     *
     * @param nextBeginOffset 下一个开始偏移量
     * @return 下一个文件的开始偏移量
     */
    long rollNextFile(final long nextBeginOffset);

    /**
     * Is the first file available?
     * @return true if it's available
     */
    boolean isFirstFileAvailable();

    /**
     * 第一个文件是否存在
     *
     * @return 第一个文件存在时返回true
     */
    boolean isFirstFileExist();
}

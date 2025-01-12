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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store;

import org.apache.rocketmq.common.annotation.PerformancePoint;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 主题队列锁，用于对主题队列的操作进行上锁，避免多线程的数据冲突问题
 * <p>
 * 使用{@link ReentrantLock}作为锁实现，这是一个排他锁
 * <p>
 * 默认为32把锁，如果配置的队列超过了32，那么需要设置{@link org.apache.rocketmq.store.config.MessageStoreConfig#topicQueueLockNum}为对应的队列数量
 */
@PerformancePoint("锁数量与队列数量不一致时，很容易出现性能问题")
public class TopicQueueLock {
    /**
     * 锁列表的大小，默认为32个
     */
    private final int size;
    /**
     * 存储锁的集合
     */
    private final List<Lock> lockList;

    public TopicQueueLock() {
        this.size = 32;
        this.lockList = new ArrayList<>(32);
        for (int i = 0; i < this.size; i++) {
            this.lockList.add(new ReentrantLock());
        }
    }

    public TopicQueueLock(int size) {
        this.size = size;
        this.lockList = new ArrayList<>(size);
        for (int i = 0; i < this.size; i++) {
            this.lockList.add(new ReentrantLock());
        }
    }

    /**
     * 加锁
     *
     * @param topicQueueKey
     */
    public void lock(String topicQueueKey) {
        Lock lock = this.lockList.get((topicQueueKey.hashCode() & 0x7fffffff) % this.size);
        lock.lock();
    }

    /**
     * 解锁
     *
     * @param topicQueueKey
     */
    public void unlock(String topicQueueKey) {
        Lock lock = this.lockList.get((topicQueueKey.hashCode() & 0x7fffffff) % this.size);
        lock.unlock();
    }
}

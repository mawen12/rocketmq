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
package org.apache.rocketmq.client.lock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于CAS的读写锁
 */
public class ReadWriteCASLock {
    //true : can lock ; false : not lock
    private final AtomicBoolean writeLock = new AtomicBoolean(true);

    private final AtomicInteger readLock = new AtomicInteger(0);

    /**
     * 申请写锁，申请成功必须满足以下两个：
     * <ul>
     *     <li>1.writeLock(true -> false)</li>
     *     <li>2.readLock == 0</li>
     * </ul>
     */
    public void acquireWriteLock() {
        boolean isLock = false;
        do {
            isLock = writeLock.compareAndSet(true, false);
        } while (!isLock);

        do {
            isLock = readLock.get() == 0;
        } while (!isLock);
    }

    /**
     * 释放写锁，writeLock(false -> true)
     */
    public void releaseWriteLock() {
        this.writeLock.compareAndSet(false, true);
    }

    /**
     * 申请读锁，申请成功仅需满足一个：
     * <ul>
     *     <li>1.writeLock == true</li>
     * </ul>
     * 申请成功之后，readLock+1
     */
    public void acquireReadLock() {
        boolean isLock = false;
        do {
            isLock = writeLock.get();
        } while (!isLock);
        readLock.getAndIncrement();
    }

    /**
     * 释放读锁，readLock-1
     */
    public void releaseReadLock() {
        this.readLock.getAndDecrement();
    }

    /**
     * 是否支持写锁，必须同时满足以下条件，writeLock=true且readLock=0
     *
     * @return
     */
    public boolean getWriteLock() {
        return this.writeLock.get() && this.readLock.get() == 0;
    }

    /**
     * 是否支持读锁，仅需满足writeLock=true
     *
     * @return
     */
    public boolean getReadLock() {
        return this.writeLock.get();
    }

}

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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public abstract class ServiceThread implements Runnable {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 运行任务内部的线程
     */
    protected Thread thread;
    /**
     * 闭锁
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 是否已通知
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 是否停止
     */
    protected volatile boolean stopped = false;
    /**
     * 是否守护线程
     */
    protected boolean isDaemon = false;

    /**
     * 使其能够重启线程
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     * 启动一个线程
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 启动状态设置失败则返回，因为可能存在其他线程将当前线程启动了的情况
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 刚启动未关闭
        stopped = false;
        // 在内部持有的线程
        this.thread = new Thread(this, getServiceName());
        // 设置是否守护线程
        this.thread.setDaemon(isDaemon);
        // 启动内部线程
        this.thread.start();
        log.info("Start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 启动状态设置失败则返回，因为可能存在其他线程将当前线程关闭了的情况
        if (!started.compareAndSet(true, false)) {
            return;
        }
        // 关闭状态设置
        this.stopped = true;
        log.info("shutdown thread[{}] interrupt={} ", getServiceName(), interrupt);

        // 如果内部线程正在等待，则唤醒
        wakeup();

        try {
            // 如果允许执行打断，则尝试对内部线程进行打断
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            // 非守护线程的停止，需要当前线程等待内部线程关闭
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            // 内部线程关闭完成的耗时，最大为JOIN_TIME
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread[{}], elapsed time: {}ms, join time:{}ms", getServiceName(), elapsedTime, this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    /**
     * 标记停止，仅仅将对应的标记信息进行更新，但是内部线程并未停止
     */
    public void makeStop() {
        // 未启动代表本身就是停止的，无需暂停
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread[{}] ", this.getServiceName());
    }

    /**
     * 唤醒内部线程
     */
    public void wakeup() {
        // 更新状态为已通知，并通知等待点
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待运行状态
     *
     * @param interval
     */
    protected void waitForRunning(long interval) {
        // 如果已通知状态可以变为未通知，直接结束
        if (hasNotified.compareAndSet(true, false)) {
            // 结束等待
            this.onWaitEnd();
            return;
        }

        // 重置状态
        waitPoint.reset();

        try {
            // 等待指定间隔，要么触发了countDown()，要么等待超时
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 将已通知状态变更为否
            hasNotified.set(false);
            // 结束等待
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}

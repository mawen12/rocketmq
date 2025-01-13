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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 重新平衡服务
 */
public class RebalanceService extends ServiceThread {

    /**
     * 等待间隔，即该线程每次开始执行前，等待指定时间
     * 从 PROPERTIES(rocketmq.client.rebalance.waitInterval) -> DEFAULT(20000)，
     */
    private static long waitInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));

    /**
     * 最小间隔，即每次重平衡失败时，等待间隔
     * 从 PROPERTIES(rocketmq.client.rebalance.minInterval) -> DEFAULT(1000)
     */
    private static long minInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.minInterval", "1000"));

    private final Logger log = LoggerFactory.getLogger(RebalanceService.class);

    private final MQClientInstance mqClientFactory;

    /**
     * 上次重新平衡的时间戳
     */
    private long lastRebalanceTimestamp = System.currentTimeMillis();

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        long realWaitInterval = waitInterval;
        while (!this.isStopped()) {
            // 等待指定间隔再开始执行
            this.waitForRunning(realWaitInterval);

            // 计算距离上次执行间隔
            long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
            if (interval < minInterval) {
                // 重新计算等待间隔，结束本次操作
                realWaitInterval = minInterval - interval;
            } else {
                // 执行重平衡
                boolean balanced = this.mqClientFactory.doRebalance();
                // 如果重平衡成功，则重置等待时间，否则等待最小时间
                realWaitInterval = balanced ? waitInterval : minInterval;
                // 更新上次平衡时间
                lastRebalanceTimestamp = System.currentTimeMillis();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}

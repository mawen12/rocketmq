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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 平均哈希队列算法，客户端默认的队列算法
 *
 * @see DefaultMQPushConsumer#DefaultMQPushConsumer(String, boolean, String)
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        List<MessageQueue> result = new ArrayList<>();
        // 未通过非空检验时，直接返回
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }
        // 获取当前消费者在所有消费者中的索引
        int index = cidAll.indexOf(currentCID);
        // 所有队列对所有消费者取模
        int mod = mqAll.size() % cidAll.size();
        /**
         * 计算平均每个消费者可以被分配的队列数
         * <ul>
         *     <li>1.如果队列总数小于消费者总数，那么每个消费者最多只能有一个队列，反之执行第二步</li>
         *     <li>2.如果队列总数不是消费者总数的整倍数，并且消费者索引小于取模值，则平均数量为count(c)/count(q) + 1，向上取；反之执行第三步</li>
         *     <li>3.count(c)/count(q)，向下取</li>
         * </ul>
         */
        int averageSize = mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());
        /**
         * 计算开始获取队列的位置，
         */
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get(startIndex + i));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}

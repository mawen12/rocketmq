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

import java.util.Map;

/**
 * 消息达到监听器
 */
public interface MessageArrivingListener {

    /**
     * 通知一个新的消息达到了一个消费队列
     *
     * @param topic 主题名称
     * @param queueId 消费队列ID
     * @param logicOffset 消费队列偏移量
     * @param tagsCode 消息标签哈希值
     * @param msgStoreTime 消息存储时间
     * @param filterBitMap 消息布隆过滤器
     * @param properties 消息属性
     */
    void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}

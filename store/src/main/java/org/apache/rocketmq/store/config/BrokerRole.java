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
package org.apache.rocketmq.store.config;

import org.apache.rocketmq.common.mawen.CorePart;

/**
 * Broker角色
 */
@CorePart(value = "不同的broker角色决定了high available的行为", part = CorePart.Part.HA)
public enum BrokerRole {
    /**
     * 异步的MASTER
     *
     * <p>在broker接受消息后，会
     */
    ASYNC_MASTER,
    /**
     * 同步的MASTER
     */
    SYNC_MASTER,
    /**
     * SLAVE
     *
     * <p>如果broker角色为slave，要做的事情
     * <ul>
     * <li>slave不允许设置brokerid < 1</li>
     * </ul>
     */
    SLAVE;
}

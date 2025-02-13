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

/**
 * $Id: MessageModel.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

import org.apache.rocketmq.common.mawen.CorePart;

/**
 * 消息模式
 */
@CorePart(value = "消费者消费消息的模式", part = CorePart.Part.CONSUMER)
public enum MessageModel {
    /**
     * 广播模式，相同Consumer Group的每个Consumer实例都接收全量的消息。
     *
     * <p>在广播模式下，消费者分组对主题下消息的消费偏移量都由消费者分组自己去做管理的。Broker不负责管理消费的偏移量。
     * 如果消费者不做管理，会导致消费消息的丢失
     */
    BROADCASTING("BROADCASTING"),
    /**
     * 集群模式，相同Consumer Group的每个Consumer实例平均分摊消息。
     *
     * <p>在集群模式下，Broker管理消费者分组消息主题下消息的偏移量，主要通过{@code ConsumerOffsetManager}实现偏移量的管理。
     */
    CLUSTERING("CLUSTERING");

    private String modeCN;

    MessageModel(String modeCN) {
        this.modeCN = modeCN;
    }

    public String getModeCN() {
        return modeCN;
    }
}

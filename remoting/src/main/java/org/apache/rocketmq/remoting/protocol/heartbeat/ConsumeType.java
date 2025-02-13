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
 * $Id: ConsumeType.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.heartbeat;

import org.apache.rocketmq.common.mawen.CorePart;

/**
 * 消费类型
 */
@CorePart(value = "消费者的消费行为", part = CorePart.Part.CONSUMER)
public enum ConsumeType {
    /**
     * 主动消费，拉模式，用于{@code PullMessageProcessor}
     *
     * <p>拉模式的实现原理是：消费者主动从broker拉取消息，然后再回调监听器
     */
    CONSUME_ACTIVELY("PULL"),

    /**
     * 被动消费，推模式，用于{@code PullMessageProcessor}
     *
     * <p>推模式的实现原理是：由后台线程池从broker拉去消息，再通知到对应的监听器实现消息的消费。推的核心点在于消息是被后台线程池推送到监听器
     */
    CONSUME_PASSIVELY("PUSH"),

    /**
     * 弹出消费，弹出模式，用于{@code PopMessageProcessor}
     */
    CONSUME_POP("POP");

    private String typeCN;

    ConsumeType(String typeCN) {
        this.typeCN = typeCN;
    }

    public String getTypeCN() {
        return typeCN;
    }
}

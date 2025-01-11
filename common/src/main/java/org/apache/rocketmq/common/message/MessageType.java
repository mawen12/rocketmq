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

package org.apache.rocketmq.common.message;

/**
 * 消息类型
 */
public enum MessageType {
    /**
     * 普通消息
     */
    Normal_Msg("Normal"),
    /**
     * 半事务消息
     */
    Trans_Msg_Half("Trans"),
    /**
     * 已提交事务消息
     */
    Trans_msg_Commit("TransCommit"),
    /**
     * 延迟消息
     */
    Delay_Msg("Delay"),
    /**
     * 顺序消息
     */
    Order_Msg("Order");

    private final String shortName;

    MessageType(String shortName) {
        this.shortName = shortName;
    }

    public String getShortName() {
        return shortName;
    }

    public static MessageType getByShortName(String shortName) {
        /**
         * TODO by mawen 可能存在内存问题
         */
        for (MessageType msgType : MessageType.values()) {
            if (msgType.getShortName().equals(shortName)) {
                return msgType;
            }
        }
        return Normal_Msg;
    }
}

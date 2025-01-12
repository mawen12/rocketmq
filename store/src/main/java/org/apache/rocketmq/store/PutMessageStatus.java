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

/**
 * 放置消息状态
 */
public enum PutMessageStatus {
    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    /**
     * 创建MappedFile失败时触发
     */
    CREATE_MAPPED_FILE_FAILED,
    /**
     * 消息属性超过了32K、消息体大小超过了4m，或者消息整体大小超过了4m+64k
     *
     * @see org.apache.rocketmq.store.config.MessageStoreConfig#maxMessageSize
     */
    MESSAGE_ILLEGAL,
    /**
     * 消息属性大小超过了32k时报错
     */
    PROPERTIES_SIZE_EXCEEDED,
    OS_PAGE_CACHE_BUSY,
    UNKNOWN_ERROR,
    IN_SYNC_REPLICAS_NOT_ENOUGH,
    PUT_TO_REMOTE_BROKER_FAIL,
    LMQ_CONSUME_QUEUE_NUM_EXCEEDED,
    WHEEL_TIMER_FLOW_CONTROL,
    /**
     * 轮式计时器信息非法
     */
    WHEEL_TIMER_MSG_ILLEGAL,
    WHEEL_TIMER_NOT_ENABLE
}

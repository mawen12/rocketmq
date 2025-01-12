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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 队列类型工具类
 */
public class QueueTypeUtils {

    /**
     * @param topicConfig
     * @return 该主题配置是否为批量队列
     */
    public static boolean isBatchCq(Optional<TopicConfig> topicConfig) {
        return Objects.equals(CQType.BatchCQ, getCQType(topicConfig));
    }

    public static CQType getCQType(Optional<TopicConfig> topicConfig) {
        /**
         * 检查主题配置是否存在
         */
        if (!topicConfig.isPresent()) {
            /**
             * 使用默认队列类型，即SimpleCQ
             */
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }

        /**
         * 获取属性名称
         */
        String attributeName = TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName();

        /**
         * 提取主题配置中的属性
         */
        Map<String, String> attributes = topicConfig.get().getAttributes();
        if (attributes == null || attributes.size() == 0) {
            /**
             * 使用默认队列类型，即SimpleCQ
             */
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }

        if (attributes.containsKey(attributeName)) {
            /**
             * 将属性解析为对应的CQType
             */
            return CQType.valueOf(attributes.get(attributeName));
        } else {
            /**
             * 使用默认队列类型，即SimpleCQ
             */
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }
    }
}
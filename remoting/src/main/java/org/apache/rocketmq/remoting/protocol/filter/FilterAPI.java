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
package org.apache.rocketmq.remoting.protocol.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Arrays;

public class FilterAPI {

    /**
     * @param topic 主题
     * @param subString 消息过滤表达式
     * @return 订阅数据
     * @throws Exception
     */
    public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {
        final SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        // 如果未指定表达式，或者表达式为*，代表订阅主题内全部信息
        if (StringUtils.isEmpty(subString) || subString.equals(SubscriptionData.SUB_ALL)) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
            return subscriptionData;
        }

        // 对表达式进行||拆分
        String[] tags = subString.split("\\|\\|");
        if (tags.length > 0) {
            Arrays.stream(tags).map(String::trim)
                    // 过滤掉空标签
                    .filter(tag -> !tag.isEmpty())
                    // 写入标签和标签哈希值
                    .forEach(tag -> {
                        subscriptionData.getTagsSet().add(tag);
                        subscriptionData.getCodeSet().add(tag.hashCode());
                    });
        } else {
            // 提供了非法的表达式，例如|
            throw new Exception("subString split error");
        }

        return subscriptionData;
    }

    public static SubscriptionData buildSubscriptionData(String topic, String subString, String expressionType) throws Exception {
        final SubscriptionData subscriptionData = buildSubscriptionData(topic, subString);
        if (StringUtils.isNotBlank(expressionType)) {
            subscriptionData.setExpressionType(expressionType);
        }
        return subscriptionData;
    }

    public static SubscriptionData build(final String topic, final String subString,
                                         final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(topic, subString);
        }

        if (StringUtils.isEmpty(subString)) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}

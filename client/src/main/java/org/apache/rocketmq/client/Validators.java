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

package org.apache.rocketmq.client;

import java.io.File;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import static org.apache.rocketmq.common.topic.TopicValidator.isTopicOrGroupIllegal;

/**
 * Common Validator
 */
public class Validators {
    public static final int CHARACTER_MAX_LENGTH = 255;
    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * 校验分组长度和字符
     */
    public static void checkGroup(String group) throws MQClientException {
        // 分组非空
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }

        // 分组长度小于255
        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }

        // 分组字符合法
        if (isTopicOrGroupIllegal(group)) {
            throw new MQClientException(String.format("the specified group[%s] contains illegal characters, allowing only %s", group, "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
        /**
         * 消息不能为空
         */
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        // topic
        /**
         * 校验主题格式，长度
         */
        Validators.checkTopic(msg.getTopic());
        /**
         * 校验主题是否允许被客户端发送消息
         */
        Validators.isNotAllowedSendTopic(msg.getTopic());

        // body
        /**
         * 消息体非空
         */
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }

        /**
         * 消息体非空
         */
        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }

        /**
         * 消息体大小不能超过4m
         */
        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }

        /**
         * 解析消息属性中的lmq路径，从msg.properties(INNER_MULTI_DISPATCH)
         * 如果格式错误，则报错
         */
        String lmqPath = msg.getUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.contains(lmqPath, File.separator)) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "INNER_MULTI_DISPATCH " + lmqPath + " can not contains " + File.separator + " character");
        }
    }

    /**
     * 校验主题格式、长度
     *
     * @param topic
     * @throws MQClientException
     */
    public static void checkTopic(String topic) throws MQClientException {
        /**
         * 主题不能为空
         */
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }

        /**
         * 主题长度不能超过127，需要注意，这个主题可能存在namespace，并且可能存在%RETRY%或%DLQ%，因此原始的主题长度更短
         */
        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }

        /**
         * 检查主题格式必须为^[%|a-zA-Z0-9_-]+$
         */
        if (isTopicOrGroupIllegal(topic)) {
            throw new MQClientException(String.format("The specified topic[%s] contains illegal characters, allowing only %s", topic, "^[%|a-zA-Z0-9_-]+$"), null);
        }
    }

    public static void isSystemTopic(String topic) throws MQClientException {
        if (TopicValidator.isSystemTopic(topic)) {
            throw new MQClientException(
                    String.format("The topic[%s] is conflict with system topic.", topic), null);
        }
    }

    public static void isNotAllowedSendTopic(String topic) throws MQClientException {
        /**
         * 如果该主题不允许被生产者发送，则报错
         */
        if (TopicValidator.isNotAllowedSendTopic(topic)) {
            throw new MQClientException(String.format("Sending message to topic[%s] is forbidden.", topic), null);
        }
    }

    public static void checkTopicConfig(final TopicConfig topicConfig) throws MQClientException {
        if (!PermName.isValid(topicConfig.getPerm())) {
            throw new MQClientException(ResponseCode.NO_PERMISSION,
                    String.format("topicPermission value: %s is invalid.", topicConfig.getPerm()));
        }
    }

    public static void checkBrokerConfig(final Properties brokerConfig) throws MQClientException {
        // TODO: use MixAll.isPropertyValid() when jdk upgrade to 1.8
        if (brokerConfig.containsKey("brokerPermission")
                && !PermName.isValid(brokerConfig.getProperty("brokerPermission"))) {
            throw new MQClientException(ResponseCode.NO_PERMISSION,
                    String.format("brokerPermission value: %s is invalid.", brokerConfig.getProperty("brokerPermission")));
        }
    }
}

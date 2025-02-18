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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 客户端发送的消息信息
 *
 * <p>消息组成，逻辑组成：
 * <ul>
 *     <li>主题: {@link #topic}</li>
 *     <li>消息体: {@link #body}</li>
 *     <li>标签：{@link #properties}TAGS</li>
 *     <li>key：{@link #properties}KEYS</li>
 * </ul>
 *
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080406428L;

    /**
     * 消息所属topic的名称，必填
     */
    private String topic;
    /**
     * 消息的系统标识
     *
     * @see org.apache.rocketmq.common.sysflag.MessageSysFlag
     */
    private int flag;
    /**
     * 消息属性，组成有：
     * <ul>
     *     <li>
     *         KEYS: 代表这条消息的业务关键词，服务器会根据keys创建哈希索引，设置后，可以在Console系统根据Topic、Keys来查询消息，
     *              由于是哈希索引，请尽可能保证key唯一，例如订单号、商品Id等。选填
     *     </li>
     *     <li>
     *         TAGS: 消息标签，方便服务器过滤使用，目前只支持每个消息设置一个tag，选填
     *     </li>
     *     <li>
     *         WAIT: 表示消息是否在服务器落盘后才返回应答
     *     </li>
     *     <li>
     *         UNIQ_KEY: 该消息的唯一ID，客户端设置
     *     </li>
     *     <li>
     *         DELAY_TIME_LEVEL：消息延时级别，0标识不延时，大于0会延时特定的时间才会被消费
     *     </li>
     * </ul>
     *
     * @see MessageClientIDSetter#setUniqID(Message)
     */
    private Map<String, String> properties;
    /**
     * 消息体，必填，最大4M
     */
    private byte[] body;
    /**
     * 事务ID
     */
    private String transactionId;

    public Message() {
    }

    public Message(String topic, byte[] body) {
        // 默认是等待消息写入到磁盘中的
        this(topic, "", "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.body = body;

        if (tags != null && tags.length() > 0) {
            this.setTags(tags);
        }

        if (keys != null && keys.length() > 0) {
            this.setKeys(keys);
        }

        this.setWaitStoreMsgOK(waitStoreMsgOK);
    }

    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }

    public void setKeys(String keys) {
        this.putProperty(MessageConst.PROPERTY_KEYS, keys);
    }

    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        this.properties.put(name, value);
    }

    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    public void putUserProperty(final String name, final String value) {
        if (MessageConst.STRING_HASH_SET.contains(name)) {
            throw new RuntimeException(String.format("The Property<%s> is used by system, input another please", name));
        }

        if (value == null || value.trim().isEmpty()
            || name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("The name or value of property can not be null or blank string!");
        }

        this.putProperty(name, value);
    }

    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        return this.properties.get(name);
    }

    public boolean hasProperty(final String name) {
        if (null == this.properties) {
            return false;
        }
        return this.properties.containsKey(name);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return this.getProperty(MessageConst.PROPERTY_TAGS);
    }

    public void setTags(String tags) {
        this.putProperty(MessageConst.PROPERTY_TAGS, tags);
    }

    public String getKeys() {
        return this.getProperty(MessageConst.PROPERTY_KEYS);
    }

    public void setKeys(Collection<String> keyCollection) {
        String keys = String.join(MessageConst.KEY_SEPARATOR, keyCollection);

        this.setKeys(keys);
    }

    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }

        return 0;
    }

    public void setDelayTimeLevel(int level) {
        this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }

    /**
     * 是否保存到磁盘上后返回
     *
     * @see org.apache.rocketmq.store.CommitLog.DefaultFlushManager
     */
    public boolean isWaitStoreMsgOK() {
        String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        if (null == result) {
            return true;
        }

        return Boolean.parseBoolean(result);
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
    }

    public void setInstanceId(String instanceId) {
        this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getBuyerId() {
        return getProperty(MessageConst.PROPERTY_BUYER_ID);
    }

    public void setBuyerId(String buyerId) {
        putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return "Message{" +
            "topic='" + topic + '\'' +
            ", flag=" + flag +
            ", properties=" + properties +
            ", body=" + Arrays.toString(body) +
            ", transactionId='" + transactionId + '\'' +
            '}';
    }

    public void setDelayTimeSec(long sec) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC, String.valueOf(sec));
    }

    public long getDelayTimeSec() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public void setDelayTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_MS, String.valueOf(timeMs));
    }

    public long getDelayTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public void setDeliverTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(timeMs));
    }

    public long getDeliverTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }
}

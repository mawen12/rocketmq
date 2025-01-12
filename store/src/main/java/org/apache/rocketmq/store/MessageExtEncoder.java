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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.nio.ByteBuffer;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class MessageExtEncoder {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private ByteBuf byteBuf;
    /**
     * 消息体最大大小，默认为4m
     */
    private int maxMessageBodySize;
    /**
     * 完整消息的最大大小，默认为4m+64k，其中的64k存储消息体之外的信息
     */
    private int maxMessageSize;
    /**
     * CRC32保留长度，默认为0；如果PROPERTIES(enabledAppendPropCRC)=true，长度为20
     */
    private final int crc32ReservedLength;
    private MessageStoreConfig messageStoreConfig;

    public MessageExtEncoder(final int maxMessageBodySize, final MessageStoreConfig messageStoreConfig) {
        this(messageStoreConfig);
    }

    public MessageExtEncoder(final MessageStoreConfig messageStoreConfig) {
        ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;
        this.messageStoreConfig = messageStoreConfig;
        this.maxMessageBodySize = messageStoreConfig.getMaxMessageSize();
        //Reserve 64kb for encoding buffer outside body
        int maxMessageSize = Integer.MAX_VALUE - maxMessageBodySize >= 64 * 1024 ? maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
        byteBuf = alloc.directBuffer(maxMessageSize);
        this.maxMessageSize = maxMessageSize;
        this.crc32ReservedLength = messageStoreConfig.isEnabledAppendPropCRC() ? CommitLog.CRC32_RESERVED_LEN : 0;
    }

    /**
     * 计算消息总长度
     * <pre>
     * ┌───────────┬───────────┬─────────┬─────────┬──────┬─────────────┬────────────────┬─────────┬───────────────┬──────────┬────────────────┬──────────────────┬────────────────┬─────────────────────────────┬─────────────────────────┬──────────────────────────────────────────────┬────────────────────────┐
     * │ TotalSize │ MagicCode │ BodyCRC │ QueueID │ Flag │ QueueOffset │ PhysicalOffset │ SysFlag │ BornTimestamp │ BornHost │ StoreTimestamp │ StoreHostAddress │ ReconsumeTimes │ Prepared Transaction Offset │ Body                    │ Topic                                        │ propertiesLength       │
     * ├───────────┼───────────┼─────────┼─────────┼──────┼─────────────┼────────────────┼─────────┼───────────────┼──────────┼────────────────┼──────────────────┼────────────────┼─────────────────────────────┼─────────────────────────┼──────────────────────────────────────────────┼────────────────────────┤
     * │ 4         │ 4         │ 4       │ 4       │ 4    │ 8           │ 8              │ 4       │ 8             │ 8|20     │ 8              │ 8|20             │ 4              │ 8                           │ 4 + max(body.length, 0) │ messageVersion.topicLengthSize + topicLength │ 2 + max(properties, 0) │
     * └───────────┴───────────┴─────────┴─────────┴──────┴─────────────┴────────────────┴─────────┴───────────────┴──────────┴────────────────┴──────────────────┴────────────────┴─────────────────────────────┴─────────────────────────┴──────────────────────────────────────────────┴────────────────────────┘
     * </pre>
     *
     * @param messageVersion
     * @param sysFlag
     * @param bodyLength
     * @param topicLength
     * @param propertiesLength
     * @return 返回消息整体长度
     */
    public static int calMsgLength(MessageVersion messageVersion, int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        /**
         * 计算发送消息的主机长度，如果消息设置了IPv6，则主机长度为20，否则使用IPv4，为8
         */
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        /**
         * 计算存储消息的主机长度，如果消息设置了IPv6，则主机长度为20，否则使用IPv4，为8
         */
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;

        return 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + bornhostLength //BORNHOST
                + 8 //STORETIMESTAMP
                + storehostAddressLength //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (Math.max(bodyLength, 0)) //BODY
                + messageVersion.getTopicLengthSize() + topicLength //TOPIC
                + 2 + (Math.max(propertiesLength, 0)); //propertiesLength
    }

    public static int calMsgLengthNoProperties(MessageVersion messageVersion,
                                               int sysFlag, int bodyLength, int topicLength) {

        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;

        return 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + bornhostLength //BORNHOST
                + 8 //STORETIMESTAMP
                + storehostAddressLength //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (Math.max(bodyLength, 0)) //BODY
                + messageVersion.getTopicLengthSize() + topicLength; //TOPIC
    }

    public PutMessageResult encodeWithoutProperties(MessageExtBrokerInner msgInner) {

        final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        final int topicLength = topicData.length;

        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

        // Exceeds the maximum message body
        if (bodyLength > this.maxMessageBodySize) {
            CommitLog.log.warn("message body size exceeded, msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageBodySize);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        final int msgLenNoProperties = calMsgLengthNoProperties(msgInner.getVersion(), msgInner.getSysFlag(), bodyLength, topicLength);

        // 1 TOTALSIZE
        this.byteBuf.writeInt(msgLenNoProperties);
        // 2 MAGICCODE
        this.byteBuf.writeInt(msgInner.getVersion().getMagicCode());
        // 3 BODYCRC
        this.byteBuf.writeInt(msgInner.getBodyCRC());
        // 4 QUEUEID
        this.byteBuf.writeInt(msgInner.getQueueId());
        // 5 FLAG
        this.byteBuf.writeInt(msgInner.getFlag());
        // 6 QUEUEOFFSET, need update later
        this.byteBuf.writeLong(0);
        // 7 PHYSICALOFFSET, need update later
        this.byteBuf.writeLong(0);
        // 8 SYSFLAG
        this.byteBuf.writeInt(msgInner.getSysFlag());
        // 9 BORNTIMESTAMP
        this.byteBuf.writeLong(msgInner.getBornTimestamp());

        // 10 BORNHOST
        ByteBuffer bornHostBytes = msgInner.getBornHostBytes();
        this.byteBuf.writeBytes(bornHostBytes.array());

        // 11 STORETIMESTAMP
        this.byteBuf.writeLong(msgInner.getStoreTimestamp());

        // 12 STOREHOSTADDRESS
        ByteBuffer storeHostBytes = msgInner.getStoreHostBytes();
        this.byteBuf.writeBytes(storeHostBytes.array());

        // 13 RECONSUMETIMES
        this.byteBuf.writeInt(msgInner.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        this.byteBuf.writeLong(msgInner.getPreparedTransactionOffset());
        // 15 BODY
        this.byteBuf.writeInt(bodyLength);
        if (bodyLength > 0)
            this.byteBuf.writeBytes(msgInner.getBody());

        // 16 TOPIC
        if (MessageVersion.MESSAGE_VERSION_V2.equals(msgInner.getVersion())) {
            this.byteBuf.writeShort((short) topicLength);
        } else {
            this.byteBuf.writeByte((byte) topicLength);
        }
        this.byteBuf.writeBytes(topicData);

        return null;
    }

    public PutMessageResult encode(MessageExtBrokerInner msgInner) {
        /**
         * 清除当前线程字节缓存区
         */
        this.byteBuf.clear();

        if (messageStoreConfig.isEnableLmq() && msgInner.needDispatchLMQ()) {
            return encodeWithoutProperties(msgInner);
        }

        /**
         * 对消息属性进行序列化
         */
        final byte[] propertiesData = msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
        /**
         * 如果crc32保留长度超过0，并且序列化后的属性不为空，且最后一位不是{@link MessageDecoder#PROPERTY_SEPARATOR}
         */
        boolean needAppendLastPropertySeparator = crc32ReservedLength > 0 && propertiesData != null && propertiesData.length > 0 && propertiesData[propertiesData.length - 1] != MessageDecoder.PROPERTY_SEPARATOR;
        /**
         * 获取最新的属性长度=原始属性长度+最后一位属性分隔符+crc32保留长度
         */
        final int propertiesLength = (propertiesData == null ? 0 : propertiesData.length) + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;

        /**
         * 检查属性长度是否超过了32k，则直接返回
         */
        if (propertiesLength > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long. length={}", propertiesLength);
            /**
             * 返回属性大小超出限制的错误
             */
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        /**
         * 对消息主题进行序列化
         */
        final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        /**
         * 主题长度
         */
        final int topicLength = topicData.length;

        /**
         * 消息体长度
         */
        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;
        /**
         * 计算消息整体长度
         */
        final int msgLen = calMsgLength(msgInner.getVersion(), msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

        /**
         * 检查消息体大小是否超过了4m，则直接返回
         */
        if (bodyLength > this.maxMessageBodySize) {
            CommitLog.log.warn("message body size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength + ", maxMessageSize: " + this.maxMessageBodySize);
            /**
             * 返回消息非法的错误
             */
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        /**
         * 获取之前设置的消息队列偏移量
         */
        final long queueOffset = msgInner.getQueueOffset();

        // Exceeds the maximum message
        /**
         * 如果消息整体大小超过了4m+64k，则直接返回
         */
        if (msgLen > this.maxMessageSize) {
            CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength + ", maxMessageSize: " + this.maxMessageSize);
            /**
             * 返回消息非法的错误
             */
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        /**
         * 1.消息整体大小
         */
        this.byteBuf.writeInt(msgLen);
        /**
         * 2.{@link MessageDecoder#MESSAGE_MAGIC_CODE}或{@link MessageDecoder#MESSAGE_MAGIC_CODE_V2}，确认主题的长度
         */
        this.byteBuf.writeInt(msgInner.getVersion().getMagicCode());
        /**
         * 3.消息体CRC
         */
        this.byteBuf.writeInt(msgInner.getBodyCRC());
        /**
         * 4.消息队列ID
         */
        this.byteBuf.writeInt(msgInner.getQueueId());
        /**
         * 5.消息的标识
         */
        this.byteBuf.writeInt(msgInner.getFlag());
        /**
         * 6.消息在队列中的偏移量
         */
        this.byteBuf.writeLong(queueOffset);
        /**
         * 7.消息在队列中的物理偏移量，之后需要更新
         */
        this.byteBuf.writeLong(0);
        /**
         * 8.消息上的系统标识
         */
        this.byteBuf.writeInt(msgInner.getSysFlag());
        /**
         * 9.消息的创建时间
         */
        this.byteBuf.writeLong(msgInner.getBornTimestamp());
        /**
         * 10.发送消息的主机信息
         */
        this.byteBuf.writeBytes(msgInner.getBornHostBytes().array());
        /**
         * 11.消息的存储时间
         */
        this.byteBuf.writeLong(msgInner.getStoreTimestamp());
        /**
         * 12.存储消息的主机信息
         */
        this.byteBuf.writeBytes(msgInner.getStoreHostBytes().array());
        /**
         * 13.消息重新消费的次数
         */
        this.byteBuf.writeInt(msgInner.getReconsumeTimes());
        /**
         * 14.预备事务偏移量
         */
        this.byteBuf.writeLong(msgInner.getPreparedTransactionOffset());
        /**
         * 15.1 消息体长度
         */
        this.byteBuf.writeInt(bodyLength);
        if (bodyLength > 0) {
            /**
             * 15.2 消息体内容
             */
            this.byteBuf.writeBytes(msgInner.getBody());
        }

        /**
         * 16.1 消息版本，V1版本的主题长度小于127，V2版本的主题长度超过127
         */
        if (MessageVersion.MESSAGE_VERSION_V2.equals(msgInner.getVersion())) {
            this.byteBuf.writeShort((short) topicLength);
        } else {
            this.byteBuf.writeByte((byte) topicLength);
        }
        /**
         * 16.2 消息主题
         */
        this.byteBuf.writeBytes(topicData);

        /**
         * 17.1消息属性长度
         */
        this.byteBuf.writeShort((short) propertiesLength);
        /**
         * 如果消息属性超度超过了20
         */
        if (propertiesLength > crc32ReservedLength) {
            /**
             * 17.2写入消息属性
             */
            this.byteBuf.writeBytes(propertiesData);
        }
        /**
         * 判断是否追加最后的属性分隔符
         */
        if (needAppendLastPropertySeparator) {
            /**
             * 17.3写入属性分隔符
             */
            this.byteBuf.writeByte((byte) MessageDecoder.PROPERTY_SEPARATOR);
        }
        /**
         * 18.写入CRC32
         */
        this.byteBuf.writerIndex(this.byteBuf.writerIndex() + crc32ReservedLength);

        return null;
    }

    public ByteBuffer encode(final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
        this.byteBuf.clear();

        ByteBuffer messagesByteBuff = messageExtBatch.wrap();

        int totalLength = messagesByteBuff.limit();
        if (totalLength > this.maxMessageBodySize) {
            CommitLog.log.warn("message body size exceeded, msg body size: " + totalLength + ", maxMessageSize: " + this.maxMessageBodySize);
            throw new RuntimeException("message body size exceeded");
        }

        // properties from MessageExtBatch
        String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
        final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
        int batchPropDataLen = batchPropData.length;
        if (batchPropDataLen > Short.MAX_VALUE) {
            CommitLog.log.warn("Properties size of messageExtBatch exceeded, properties size: {}, maxSize: {}.", batchPropDataLen, Short.MAX_VALUE);
            throw new RuntimeException("Properties size of messageExtBatch exceeded!");
        }
        final short batchPropLen = (short) batchPropDataLen;

        int batchSize = 0;
        while (messagesByteBuff.hasRemaining()) {
            batchSize++;
            // 1 TOTALSIZE
            messagesByteBuff.getInt();
            // 2 MAGICCODE
            messagesByteBuff.getInt();
            // 3 BODYCRC
            messagesByteBuff.getInt();
            // 4 FLAG
            int flag = messagesByteBuff.getInt();
            // 5 BODY
            int bodyLen = messagesByteBuff.getInt();
            int bodyPos = messagesByteBuff.position();
            int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
            messagesByteBuff.position(bodyPos + bodyLen);
            // 6 properties
            short propertiesLen = messagesByteBuff.getShort();
            int propertiesPos = messagesByteBuff.position();
            messagesByteBuff.position(propertiesPos + propertiesLen);
            boolean needAppendLastPropertySeparator = propertiesLen > 0 && batchPropLen > 0
                    && messagesByteBuff.get(messagesByteBuff.position() - 1) != MessageDecoder.PROPERTY_SEPARATOR;

            final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

            final int topicLength = topicData.length;
            int totalPropLen = needAppendLastPropertySeparator ?
                    propertiesLen + batchPropLen + 1 : propertiesLen + batchPropLen;

            // properties need to add crc32
            totalPropLen += crc32ReservedLength;
            final int msgLen = calMsgLength(
                    messageExtBatch.getVersion(), messageExtBatch.getSysFlag(), bodyLen, topicLength, totalPropLen);

            // 1 TOTALSIZE
            this.byteBuf.writeInt(msgLen);
            // 2 MAGICCODE
            this.byteBuf.writeInt(messageExtBatch.getVersion().getMagicCode());
            // 3 BODYCRC
            this.byteBuf.writeInt(bodyCrc);
            // 4 QUEUEID
            this.byteBuf.writeInt(messageExtBatch.getQueueId());
            // 5 FLAG
            this.byteBuf.writeInt(flag);
            // 6 QUEUEOFFSET
            this.byteBuf.writeLong(0);
            // 7 PHYSICALOFFSET
            this.byteBuf.writeLong(0);
            // 8 SYSFLAG
            this.byteBuf.writeInt(messageExtBatch.getSysFlag());
            // 9 BORNTIMESTAMP
            this.byteBuf.writeLong(messageExtBatch.getBornTimestamp());

            // 10 BORNHOST
            ByteBuffer bornHostBytes = messageExtBatch.getBornHostBytes();
            this.byteBuf.writeBytes(bornHostBytes.array());

            // 11 STORETIMESTAMP
            this.byteBuf.writeLong(messageExtBatch.getStoreTimestamp());

            // 12 STOREHOSTADDRESS
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes();
            this.byteBuf.writeBytes(storeHostBytes.array());

            // 13 RECONSUMETIMES
            this.byteBuf.writeInt(messageExtBatch.getReconsumeTimes());
            // 14 Prepared Transaction Offset, batch does not support transaction
            this.byteBuf.writeLong(0);
            // 15 BODY
            this.byteBuf.writeInt(bodyLen);
            if (bodyLen > 0)
                this.byteBuf.writeBytes(messagesByteBuff.array(), bodyPos, bodyLen);

            // 16 TOPIC
            if (MessageVersion.MESSAGE_VERSION_V2.equals(messageExtBatch.getVersion())) {
                this.byteBuf.writeShort((short) topicLength);
            } else {
                this.byteBuf.writeByte((byte) topicLength);
            }
            this.byteBuf.writeBytes(topicData);

            // 17 PROPERTIES
            this.byteBuf.writeShort((short) totalPropLen);
            if (propertiesLen > 0) {
                this.byteBuf.writeBytes(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            if (batchPropLen > 0) {
                if (needAppendLastPropertySeparator) {
                    this.byteBuf.writeByte((byte) MessageDecoder.PROPERTY_SEPARATOR);
                }
                this.byteBuf.writeBytes(batchPropData, 0, batchPropLen);
            }
            this.byteBuf.writerIndex(this.byteBuf.writerIndex() + crc32ReservedLength);
        }
        putMessageContext.setBatchSize(batchSize);
        putMessageContext.setPhyPos(new long[batchSize]);

        return this.byteBuf.nioBuffer();
    }

    public ByteBuffer getEncoderBuffer() {
        return this.byteBuf.nioBuffer(0, this.byteBuf.capacity());
    }

    public int getMaxMessageBodySize() {
        return this.maxMessageBodySize;
    }

    public void updateEncoderBufferCapacity(int newMaxMessageBodySize) {
        this.maxMessageBodySize = newMaxMessageBodySize;
        //Reserve 64kb for encoding buffer outside body
        this.maxMessageSize = Integer.MAX_VALUE - newMaxMessageBodySize >= 64 * 1024 ?
                this.maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
        this.byteBuf.capacity(this.maxMessageSize);
    }

    static class PutMessageThreadLocal {
        private final MessageExtEncoder encoder;
        private final StringBuilder keyBuilder;

        PutMessageThreadLocal(MessageStoreConfig messageStoreConfig) {
            encoder = new MessageExtEncoder(messageStoreConfig);
            keyBuilder = new StringBuilder();
        }

        public MessageExtEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }

}

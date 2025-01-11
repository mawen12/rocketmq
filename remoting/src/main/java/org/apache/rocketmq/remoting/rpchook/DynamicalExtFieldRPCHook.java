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
package org.apache.rocketmq.remoting.rpchook;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 基于动态扩展字段的{@link RPCHook}实现
 */
public class DynamicalExtFieldRPCHook implements RPCHook {

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        /**
         * 获取zone名称，从 PROPERTIES(rocketmq.zone) -> ENV(ROCKETMQ_ZONE)
         */
        String zoneName = System.getProperty(MixAll.ROCKETMQ_ZONE_PROPERTY, System.getenv(MixAll.ROCKETMQ_ZONE_ENV));
        if (StringUtils.isNotBlank(zoneName)) {
            /**
             * 向请求头添加{@link __ZONE_NAME}头信息
             */
            request.addExtField(MixAll.ZONE_NAME, zoneName);
        }
        /**
         * 获取zone模式，从 PROPERTIES(rocketmq.zone.mode) -> ENV(ROCKETMQ_ZONE_MODE)
         */
        String zoneMode = System.getProperty(MixAll.ROCKETMQ_ZONE_MODE_PROPERTY, System.getenv(MixAll.ROCKETMQ_ZONE_MODE_ENV));
        if (StringUtils.isNotBlank(zoneMode)) {
            /**
             * 向请求头添加{@link __ZONE_MODE}头信息
             */
            request.addExtField(MixAll.ZONE_MODE, zoneMode);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
    }
}

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
package org.apache.rocketmq.common;

/**
 * 服务状态
 */
public enum ServiceState {
    /**
     * 刚创建，未启动
     */
    CREATE_JUST,
    /**
     * 运行中
     *
     * @see org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#start(boolean)
     * @see org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#start()
     */
    RUNNING,
    /**
     * 关闭
     *
     * @see org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#shutdown(boolean)
     * @see org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#shutdown(long)
     */
    SHUTDOWN_ALREADY,
    /**
     * 启动失败
     */
    START_FAILED;
}

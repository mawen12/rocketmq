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
 * commitlog的存储类型
 *
 * @see org.apache.rocketmq.store.config.MessageStoreConfig
 */
public enum StoreType {
    /**
     * 默认类型，即使用本地磁盘存储commitlog
     *
     * <p>默认的存储路径为$HOME/store/commitlog
     */
    DEFAULT("default"),
    /**
     * 使用ROCKSDB来存储文件
     *
     * <p>RocksDB是Facebook开源的，提供内置的、持久化键值存储的工具
     *
     * @see <a href="https://github.com/facebook/rocksdb/">rocksdb</a>
     */
    DEFAULT_ROCKSDB("defaultRocksDB");

    // TODO by mawen 枚举的值是不可变的，因此建议设置为final
    private String storeType;

    StoreType(String storeType) {
        this.storeType = storeType;
    }

    public String getStoreType() {
        return storeType;
    }
}

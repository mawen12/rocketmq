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
package org.apache.rocketmq.store.config;

import java.io.File;

public class StorePathConfigHelper {

    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/consumequeue
     */
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }

    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/consumequeue_ext
     */
    public static String getStorePathConsumeQueueExt(final String rootDir) {
        return rootDir + File.separator + "consumequeue_ext";
    }
    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/batchconsumequeue
     */

    public static String getStorePathBatchConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "batchconsumequeue";
    }

    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/index
     */
    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }

    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/checkpoint
     */
    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }

    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/abort
     */
    public static String getAbortFile(final String rootDir) {
        return rootDir + File.separator + "abort";
    }

    /**
     * @param rootDir 根路径，如果未设置，则为$HOME/store
     * @return 默认位于$HOME/store/lock
     */
    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }

    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }

    public static String getTranStateTableStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "statetable";
    }

    public static String getTranRedoLogStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "redolog";
    }

}

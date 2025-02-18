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

package org.apache.rocketmq.remoting.protocol.body;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * high available 运行时信息
 *
 * <p>master和slave之间的运行时信息
 */
public class HARuntimeInfo extends RemotingSerializable {

    /**
     * 是否为master节点
     */
    private boolean master;

    /**
     * master节点上commitlog的最大物理偏移量
     */
    private long masterCommitLogMaxOffset;

    /**
     * 处于同步中的slave数量
     */
    private int inSyncSlaveNums;

    /**
     * master的连接运行时信息列表
     */
    private List<HAConnectionRuntimeInfo> haConnectionInfo = new ArrayList<>();

    /**
     * high available 客户端运行时信息
     */
    private HAClientRuntimeInfo haClientRuntimeInfo = new HAClientRuntimeInfo();

    public boolean isMaster() {
        return this.master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public long getMasterCommitLogMaxOffset() {
        return this.masterCommitLogMaxOffset;
    }

    public void setMasterCommitLogMaxOffset(long masterCommitLogMaxOffset) {
        this.masterCommitLogMaxOffset = masterCommitLogMaxOffset;
    }

    public int getInSyncSlaveNums() {
        return this.inSyncSlaveNums;
    }

    public void setInSyncSlaveNums(int inSyncSlaveNums) {
        this.inSyncSlaveNums = inSyncSlaveNums;
    }

    public List<HAConnectionRuntimeInfo> getHaConnectionInfo() {
        return this.haConnectionInfo;
    }

    public void setHaConnectionInfo(List<HAConnectionRuntimeInfo> haConnectionInfo) {
        this.haConnectionInfo = haConnectionInfo;
    }

    public HAClientRuntimeInfo getHaClientRuntimeInfo() {
        return this.haClientRuntimeInfo;
    }

    public void setHaClientRuntimeInfo(HAClientRuntimeInfo haClientRuntimeInfo) {
        this.haClientRuntimeInfo = haClientRuntimeInfo;
    }

    /**
     * high available 连接运行时信息
     */
    public static class HAConnectionRuntimeInfo extends RemotingSerializable {
        /**
         * 目标连接地址
         */
        private String addr;
        /**
         * slave确认的偏移量
         */
        private long slaveAckOffset;
        /**
         * master与slave的偏移量差值
         */
        private long diff;
        /**
         * 是否处于正在同步状态
         */
        private boolean inSync;
        /**
         * 每秒已传输的字节数
         */
        private long transferredByteInSecond;
        /**
         * 从哪里传输的
         */
        private long transferFromWhere;

        public String getAddr() {
            return this.addr;
        }

        public void setAddr(String addr) {
            this.addr = addr;
        }

        public long getSlaveAckOffset() {
            return this.slaveAckOffset;
        }

        public void setSlaveAckOffset(long slaveAckOffset) {
            this.slaveAckOffset = slaveAckOffset;
        }

        public long getDiff() {
            return this.diff;
        }

        public void setDiff(long diff) {
            this.diff = diff;
        }

        public boolean isInSync() {
            return this.inSync;
        }

        public void setInSync(boolean inSync) {
            this.inSync = inSync;
        }

        public long getTransferredByteInSecond() {
            return this.transferredByteInSecond;
        }

        public void setTransferredByteInSecond(long transferredByteInSecond) {
            this.transferredByteInSecond = transferredByteInSecond;
        }

        public long getTransferFromWhere() {
            return transferFromWhere;
        }

        public void setTransferFromWhere(long transferFromWhere) {
            this.transferFromWhere = transferFromWhere;
        }
    }

    public static class HAClientRuntimeInfo extends RemotingSerializable {
        /**
         * master节点地址
         */
        private String masterAddr;
        /**
         * 每秒已传输的字节数
         */
        private long transferredByteInSecond;
        /**
         * 最大物理偏移量
         */
        private long maxOffset;
        /**
         * 最后读取的时间戳
         */
        private long lastReadTimestamp;
        /**
         * 最后写入的时间戳
         */
        private long lastWriteTimestamp;
        /**
         * master节点刷新的偏移量
         */
        private long masterFlushOffset;
        /**
         * 是否已经激活
         */
        private boolean isActivated = false;

        public String getMasterAddr() {
            return this.masterAddr;
        }

        public void setMasterAddr(String masterAddr) {
            this.masterAddr = masterAddr;
        }

        public long getTransferredByteInSecond() {
            return this.transferredByteInSecond;
        }

        public void setTransferredByteInSecond(long transferredByteInSecond) {
            this.transferredByteInSecond = transferredByteInSecond;
        }

        public long getMaxOffset() {
            return this.maxOffset;
        }

        public void setMaxOffset(long maxOffset) {
            this.maxOffset = maxOffset;
        }

        public long getLastReadTimestamp() {
            return this.lastReadTimestamp;
        }

        public void setLastReadTimestamp(long lastReadTimestamp) {
            this.lastReadTimestamp = lastReadTimestamp;
        }

        public long getLastWriteTimestamp() {
            return this.lastWriteTimestamp;
        }

        public void setLastWriteTimestamp(long lastWriteTimestamp) {
            this.lastWriteTimestamp = lastWriteTimestamp;
        }

        public long getMasterFlushOffset() {
            return masterFlushOffset;
        }

        public void setMasterFlushOffset(long masterFlushOffset) {
            this.masterFlushOffset = masterFlushOffset;
        }
    }

}

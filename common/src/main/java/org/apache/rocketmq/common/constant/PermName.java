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
package org.apache.rocketmq.common.constant;

/**
 * 权限名称
 */
public class PermName {
    public static final int INDEX_PERM_PRIORITY = 3;
    public static final int INDEX_PERM_READ = 2;
    public static final int INDEX_PERM_WRITE = 1;
    public static final int INDEX_PERM_INHERIT = 0;

    /**
     * 权限优先级，1 << 3 = 8
     */
    public static final int PERM_PRIORITY = 0x1 << INDEX_PERM_PRIORITY;
    /**
     * 读权限，1 << 2 = 4
     */
    public static final int PERM_READ = 0x1 << INDEX_PERM_READ;
    /**
     * 写权限，1 << 1 = 2
     */
    public static final int PERM_WRITE = 0x1 << INDEX_PERM_WRITE;
    /**
     * 权限继承，1 << 0 = 1
     * 如果发送消息时携带的主题不存在，则基于默认主题TBW102的权限创建新主题
     */
    public static final int PERM_INHERIT = 0x1 << INDEX_PERM_INHERIT;

    public static String perm2String(final int perm) {
        final StringBuilder sb = new StringBuilder("---");
        if (isReadable(perm)) {
            sb.replace(0, 1, "R");
        }

        if (isWriteable(perm)) {
            sb.replace(1, 2, "W");
        }

        if (isInherited(perm)) {
            sb.replace(2, 3, "X");
        }

        return sb.toString();
    }

    /**
     * @param perm
     * @return 是否为读权限
     */
    public static boolean isReadable(final int perm) {
        return (perm & PERM_READ) == PERM_READ;
    }

    /**
     * @param perm
     * @return 是否为写权限
     */
    public static boolean isWriteable(final int perm) {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }

    /**
     * @param perm
     * @return 是否继承权限
     */
    public static boolean isInherited(final int perm) {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }

    /**
     * @param perm
     * @return 是否合法
     */
    public static boolean isValid(final String perm) {
        return isValid(Integer.parseInt(perm));
    }

    /**
     * @param perm
     * @return 是否合法
     */
    public static boolean isValid(final int perm) {
        return perm >= 0 && perm < PERM_PRIORITY;
    }

    /**
     * @param perm
     * @return 是否优先级
     */
    public static boolean isPriority(final int perm) {
        return (perm & PERM_PRIORITY) == PERM_PRIORITY;
    }

    /**
     * @param perm
     * @return 是否可访问，可读或可写代表可访问
     */
    public static boolean isAccessible(final int perm) {
        return isReadable(perm) || isWriteable(perm);
    }
}

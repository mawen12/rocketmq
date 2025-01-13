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

package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.common.annotation.ImportantPoint;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

/**
 * 可控制偏移量，封装了支持原子更新、线程安全的偏移量。该类支持冻结偏移量，在被冻结前无法对偏移量执行更新。
 * <p>
 * 并发场景：
 * <ul>
 *     <li>
 *         如果方法{@link #updateAndFreeze(long)}在其他{@code update}方法之前调用，将导致{@link #allowToUpdate}=false，且会将偏移量更新为指定的目标值，
 *         在该操作后，后续调用的{@code update}将不会生产任何影响，因为它已经被冻结
 *     </li>
 *     <li>
 *         如果{@code update}和{@link #updateAndFreeze(long)}同时调用，最终结果取决于以下操作顺序：
 *         <ol>
 *             1.如果{@code update}的原子操作先于{@link #updateAndFreeze(long)}完成，后者操作将覆盖偏移量，并设置{@link #allowToUpdate}=false，阻止后续操作
 *         </ol>
 *         <ol>
 *             2.如果{@link #updateAndFreeze(long)}的原子操作在{@code update}之前完成，正在进行的{@code update}将不会继续更新，
 *             两个操作中使用{@link AtomicLong#getAndUpdate(LongUnaryOperator)}方法确保方法原子性并遵守由{@link #updateAndFreeze(long)}施加的最终状态，
 *             即使{@code update}已经开始
 *         </ol>
 *     </li>
 *     <li>
 *         从本质上来说，一旦{@link #updateAndFreeze(long)}开始执行，由于{@link #allowToUpdate}状态变化具有及时可见性，
 *         因此偏移量值对于任何后续{@code update}保持不变，这得益于其易变性
 *     </li>
 * </ul>
 * <p>
 * 基于原子类的偏移量和基于volatile的布尔值的组合提供了一种在并发环境下管理偏移量的可靠机制。
 * <p>
 * 需要注意的是，其仅提供了冻结，并未提供解除冻结的方法。
 *
 */
@ImportantPoint("消费者消费偏移量记录")
public class ControllableOffset {
    /**
     * 偏移量，基于原子类，确保并发更新的安全性
     */
    private final AtomicLong value;
    /**
     * 控制是否允许更新偏移量，默认为true，即允许更新
     */
    private volatile boolean allowToUpdate;

    public ControllableOffset(long value) {
        this.value = new AtomicLong(value);
        this.allowToUpdate = true;
    }

    /**
     * 尝试更新偏移量到目标值，如果是仅增量更新，这偏移量不会被减少。该操作是原子且线程安全的。
     * 该操作将检查{@link #allowToUpdate}，如果已经被之前调用的{@link #updateAndFreeze(long)}所冻结，
     * 则方法不会更新偏移量
     *
     * @param target       the new target offset value.
     * @param increaseOnly if true, the offset will only be updated if the target value
     *                     is greater than the current value.
     */
    public void update(long target, boolean increaseOnly) {
        // 如果尚未被冻结，则可以更新
        if (allowToUpdate) {
            value.getAndUpdate(val -> {
                // 双重校验，未被冻结才可以更新
                if (allowToUpdate) {
                    if (increaseOnly) {
                        // 如果是增量更新，则取两者较大值
                        return Math.max(target, val);
                    } else {
                        // 反之直接更新
                        return target;
                    }
                } else {
                    // 不允许更新时，返回当前值
                    return val;
                }
            });
        }
    }

    /**
     * 覆盖更新，并遵守{@link #update(long, boolean)}对于{@link #allowToUpdate}的行为规则
     *
     * @param target The new target value for the offset.
     */
    public void update(long target) {
        // 覆盖更新
        update(target, false);
    }

    /**
     * 使用提供的参数冻结偏移量，一旦冻结，后续调用的{@link #update(long, boolean)}将不会更新偏移量。
     * 该方法还会导致{@link #allowToUpdate}=false，确保偏移量始终是当前状态。
     *
     * @param target the new target offset value to freeze at.
     */
    public void updateAndFreeze(long target) {
        // 冻结更新
        value.getAndUpdate(val -> {
            // 更新volatile字段
            allowToUpdate = false;
            //
            return target;
        });
    }

    /**
     * 返回偏移量
     * @return
     */
    public long getOffset() {
        return value.get();
    }
}

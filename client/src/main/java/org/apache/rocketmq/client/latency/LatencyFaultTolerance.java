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

package org.apache.rocketmq.client.latency;

public interface LatencyFaultTolerance<T> {
    /**
     * 更新失败条目
     * @param name brokerName
     * @param currentLatency 消息发送故障延迟时间
     * @param notAvailableDuration 不可用持续时长，在这段时间内，Broker将被规避
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断Broker是否可用 当前系统时间加上需要规避的时长
     * eg：比如当前是2022.4.11 17:36:00 需要规避的时长是30s，那么这里就时当前时间戳-30<0，不可用；等到broker故障回复，这个时间就时0，表示可用
     * @param name brokerName名称
     * @return
     */
    boolean isAvailable(final T name);

    /**
     * 移除Fault条目，意味着Broker重新参与路由计算
     * @param name
     */
    void remove(final T name);

    /**
     * 尝试从规避的broker中选择一个可用的Broker，如果没有找到，返回null
     * @return
     */
    T pickOneAtLeast();
}

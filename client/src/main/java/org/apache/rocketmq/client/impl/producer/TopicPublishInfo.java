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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    /**
     * 是否是顺序消费
     */
    private boolean orderTopic = false;
    /**
     * 是否有队列路由的信息
     */
    private boolean haveTopicRouterInfo = false;
    /**
     * 该主题下的消息队列
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

    /**
     * 每选择一次消息队列，该值自增1，如果Integer.MAX_VALUE，则重置为0，用于选择消息队列
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * 默认的 不启用Broker故障延迟机制
     * 该算法在一次消息发送过程中能成功规避故障的Broker，但是如果Broker宕机，由于路由算法中的消息队列是按照Broker排序的，
     * 如果上一次根据由算法选择的是宕机的Broker的第一个队列，那么随后的下次选择的是宕机Broker的第二个队列，消息发送很可能再次失败，再次引发重试，带来不必要的性能损耗
     * 那么有什么方法在一次消息发送失败后，暂时将该broker排除在消息队列选择范围外呢？出现这个问题是因为NameServer检查Broker是否可用是有延迟的，最短的一次心跳间隔时间是10s；
     * 其次，NameServer不会监测到Broker宕机后马上推送消息给消息生产者，而是消息生产者每隔30s更新一次路由信息，所以消息生产者最快感知Broker的最新路由信息也需要30s。
     * 如果能引入一种机制，在Broker宕机期间，如果一次消息发送失败后，可以将改Broker暂时排除在消息队列的选择范围中即可。
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        // 艾斯：[消息发送] 选择消息队列：如果上一次选择队列时，发送失败，那么下次选择broker时，将上次失败的broker排除在外；
        // 如果 lastBrokerName 为null，则用sendWhichQueue自增再获取值，余当前路由表中消息队列个数取模，返回该位置的MessageQueue
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}

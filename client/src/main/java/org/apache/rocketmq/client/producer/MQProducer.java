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
package org.apache.rocketmq.client.producer;

import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

public interface MQProducer extends MQAdmin {
    void start() throws MQClientException;

    void shutdown();

    /**
     * 查找主题下所有的消息队列
     * @param topic 主题
     * @return
     * @throws MQClientException
     */
    List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;

    /**
     * 同步发送消息，消息发送到哪个消息队列由负载算法决定
     * @param msg
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    /**
     * 同步发送队列，如果发送超时timeout则抛出超时异常
     * @param msg
     * @param timeout
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final long timeout) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送消息，SendCallback：发送成功的回调方法
     * @param msg
     * @param sendCallback
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
        RemotingException, InterruptedException;

    /**
     * 异步发送消息，如果超过timeout位发送成功，则抛出异常
     * @param msg
     * @param sendCallback
     * @param timeout
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void send(final Message msg, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException;

    /**
     * 单向发送消息，不关心发送结果，只要发送出去该方法立刻返回
     * @param msg
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void sendOneway(final Message msg) throws MQClientException, RemotingException,
        InterruptedException;

    /**
     * 同步发送消息至指定队列
     * @param msg
     * @param mq
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final MessageQueue mq) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    /**
     * 同步发送消息至指定队列，若timeout未发送成功则抛出异常
     * @param msg
     * @param mq
     * @param timeout
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    /**
     * 异步发送消息至制定队列，SendCallback发送成功回调函数
     * @param msg
     * @param mq
     * @param sendCallback
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException;

    /**
     * 异步发送消息至制定队列，超时异常
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException;

    /**
     * 单向发送消息至制定队列
     * @param msg
     * @param mq
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException,
        RemotingException, InterruptedException;

    /**
     * 消息发送指定消息选择算法，覆盖生产者默认的消息队列负载
     * @param msg
     * @param selector
     * @param arg
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback) throws MQClientException, RemotingException,
        InterruptedException;

    void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
        InterruptedException;

    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
        throws MQClientException, RemotingException, InterruptedException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter tranExecuter, final Object arg) throws MQClientException;

    TransactionSendResult sendMessageInTransaction(final Message msg,
        final Object arg) throws MQClientException;

    //for batch
    SendResult send(final Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    SendResult send(final Collection<Message> msgs, final long timeout) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
    
    void send(final Collection<Message> msgs, final SendCallback sendCallback) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException;
    
    void send(final Collection<Message> msgs, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;
    
    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback) throws MQClientException, RemotingException,
        MQBrokerException, InterruptedException;
    
    void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws MQClientException,
        RemotingException, MQBrokerException, InterruptedException;
    
    //for rpc
    Message request(final Message msg, final long timeout) throws RequestTimeoutException, MQClientException,
        RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final RequestCallback requestCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException,
        InterruptedException;

    void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback,
        final long timeout) throws MQClientException, RemotingException,
        InterruptedException, MQBrokerException;

    Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException;

    void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException;
}

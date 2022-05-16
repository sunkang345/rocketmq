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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * RocketMQ主从同步核心实现类
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 该方法在 Master 收到从服务器的拉取请求后被调用，表示从服务器当前已同步的 偏移量，既然收到从服务器的反馈信息，需要唤醒某些消息发送者线程。
     * 如果从服务器 收到的确认偏移量大于 push2SlaveMaxOffs巳t，则更新 push2SlaveMaxOffset，然后唤醒 GroupTransferService 线程，
     * 各消息发送者线程再次判断自己本次发送的消息是否已经成功复制到从服务器。
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    /**
     * HAService 整体工作机制
     1 ）主服务器启动，并在特定端口上监昕从服务器的连接。
     2 ）从服务器主动连接主服务器，主服务器接收客户端的连接，并建立相关 TCP 连接。
     3 ）从服务器主动向主服务器发送待拉取消息偏移量，主服务器解析请求并返回消息给从服务器。
     4 ）从服务器保存消息并继续发送新的消息同步请求。
     */
    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     * HA Master端监听客户端连接实现类
     */
    class AcceptSocketService extends ServiceThread {
        /**
         * Broker服务端监听套接字（本地IP+端口号）
         */
        private final SocketAddress socketAddressListen;
        /**
         * 服务端Socket通道,基于NIO
         */
        private ServerSocketChannel serverSocketChannel;

        /**
         * 事件选择器，基于NIO
         */
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         * 创建 ServerSocketChannel 、 创建 Selector、设置 TCP reuseAddress 、 绑定监昕端口、设 置为非阻塞模式，并注册 OP ACCEPT （连接事件） 。
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         * 该方法是标准的基于 NIO 的服务端程式实例，选择器每 1s 处理一次连接就绪事件。 连 接事件就绪后，调用 ServerSocketChannel 的 accept（）方法创建 SocketChannel 。
         * 然后为每一个连接创建一个 HAConnection 对象 ， 该 HAConnection 将负责 M-S 数据同步逻辑。
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     * 主从同步通知实现类
     * GroupTransferService 主从同步阻塞实现，如果是同步主从模式，消息发送者将消 息刷写到磁盘后，需要继续等待新数据被传输到从服务器，
     * 从服务器数据的复制是在另 外一个线程 HAConnection 中去拉取，所以消息发送者在这里需要等待数据传输的结果， GroupTransferService 就是实现该功能，
     * 该类的整体结构与同步刷盘实现类（ CommitLogGroupCommitService）类似，本节只关注该类的核心业务逻辑 doWaitTransfer 的实现
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

        public void putRequest(final CommitLog.GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        /**
         * GroupTransferService 的职责是负责当主从同步复制结束后通知由于等待HA同步结果而阻塞的消息发送者线程。
         * 判断主从同步是否完成的依据是 Slave中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量，
         * 如果是则 表示主从同步复制已经完成，唤醒消息发送线程，否则等待1s再次判断，每一个任务在一批任务中循环判断 5次。
         * 消息发送者返回有两种情况：等待超过5s或GroupTransferService通知主从复制完成。 可以通过 syncFlushTimeout 来设置发送线程等待超时时间
         */
        private void doWaitTransfer() {
            if (!this.requestsRead.isEmpty()) {
                for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                    boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    long deadLine = req.getDeadLine();
                    while (!transferOK && deadLine - System.nanoTime() > 0) {
                        this.notifyTransferObject.waitForRunning(1000);
                        transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                    }

                    req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                }

                this.requestsRead = new LinkedList<>();
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HA Client端实现类
     * 主从同步Salve端的核心实现类
     */
    class HAClient extends ServiceThread {
        /**
         * socket读缓存区的带下
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**
         * master地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /**
         * salve向master发起主从同步的拉取偏移量
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        /**
         * 网络传输通道
         */
        private SocketChannel socketChannel;
        /**
         * nio事件选择器
         */
        private Selector selector;
        /**
         * 上一次写入的时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 反馈salve当前的复制进度，commitlog文件的最大偏移量
         */
        private long currentReportedOffset = 0;
        /**
         * 本次已处理缓存区的指针
         */
        private int dispatchPosition = 0;
        /**
         * 读缓存区 4m
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * 读缓存区备份 与byteBufferRead进行数据交换
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * Step5 ：处理网络读请求，即处理从 Master 服务器传回的消息数据。 同样 RocketMQ 作 者给出了一个处理网络读的 NIO 示例。
         * 循环判断 readByteBuffer 是否还有剩余空间，如果 存在剩余空间，则调用 SocketChannel#read ( ByteBuff，readByteBuffer），将通道中的数据 读入到读缓存区中。
         * 1 ）如果读取到的字节数大于 0，重置i卖取到 0 字节的次数，并更新最后一次写入时间
         * 戳（ lastWriteTimestamp），然后调用 dispatchReadRequest 方法将读取到的所有消息全部追 加到消息内存映射文件中，然后再次反馈拉取进度给服务器。
         * 2 ）如果连续 3 次从网络通道读取到 0 个字节，则结束本次读，返回 true 。
         * 3 ）如果读取到的字节数小于 0 或发生 IO 异常，则返回 false 。
         * HA Client 线程反复执行上述 5 个步骤完成主从同步复制功能。
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = byteBufferRead.array();
                        int dataStart = this.dispatchPosition + msgHeaderSize;

                        HAService.this.defaultMessageStore.appendToCommitLog(
                                masterPhyOffset, bodyData, dataStart, bodySize);

                        this.dispatchPosition += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * 艾斯：[HAClient工作原理] step1 salve服务器连接master服务器。
         * Slave 服务器连接 Master 服务器。 如果 socketChannel 为 空， 则 尝试连接 Master。
         * 如果 master 地址为空，返回 false ；
         * 如果 master 地址不为空，则建立到 Master 的 TCP 连接， 然后注册 OP_READ （网络读事件），
         * 初始化 currentR巳portedOffset 为 commitlog 文件的最大偏移量 、 lastWriteTimestamp 上次写人时间戳为当前时间戳，并返回 true 。
         * 在 Broker 启动时，如果 Broker 角色为 SLAVE 时将读取 Broker 配置文件中的 haMasterAddress 属性并更新 HAClient 的 masterAddrees，
         * 如果角色为 SLAVE 并且 haMasterAddress 为空 ，
         * 启动并不会报错，但不会执行主从同步复制， 该方法最终返回是否成功连接上 Master。
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /*
                     艾斯：[HAClient工作原理] step1 salve服务器连接master服务器。
                     */
                    if (this.connectMaster()) {

                        /*
                         艾斯：[HAClient工作原理] step2 判断是否需要向master反馈当前待拉取偏移量
                         Master 与 Slave 的 HA 心跳发 送间隔默认为缸，可通过配置 haSendHeartbeatlnterval 来改变心跳间隔。
                         */
                        if (this.isTimeToReportOffset()) {
                            /*
                             艾斯：[HAClient工作原理] step3
                             向 Master 服务器反馈拉取偏移量。 这里有两重意义 ， 对于 Slave 端来说，是发 送下次待拉取消息偏移量，
                             而对于 Master 服务端来说，既可以认为是 Slave 本次请求拉取的消息偏移量，也可以理解为 Slave 的消息同步 ACK 确认消息。
                             */
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        /*
                         艾斯：[HAClient工作原理] step4 进行事件选择，其执行间隔为1s
                         */
                        this.selector.select(1000);

                        /*
                         艾斯：[HAClient工作原理] step5
                         */
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}

package com.sohu.redis.net;

import com.sohu.redis.operation.Operation;
import com.sohu.redis.operation.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class RedisConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnection.class);

    private LinkedBlockingQueue<Operation> pendingQueue = new LinkedBlockingQueue<Operation>();

    private LinkedBlockingQueue<Operation> writeQueue = new LinkedBlockingQueue<Operation>();

    private ByteBuffer rbuf;

    private ByteBuffer wbuf;

    private String host;

    private int port;

    private SocketChannel socketChannel;

    private TCPComponent tcpComponent;

    private int rBufSize = 4096;

    private int wBufSize = 4096;

    private ReentrantLock writeLock = new ReentrantLock();

    public RedisConnection(String host, int port) {
        this.host = host;
        this.port = port;
        rbuf = ByteBuffer.allocate(rBufSize);
        wbuf = ByteBuffer.allocate(wBufSize);
    }

    public int getrBufSize() {
        return rBufSize;
    }

    public void setrBufSize(int size) {
        this.rBufSize = size;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public TCPComponent getTcpComponent() {
        return tcpComponent;
    }

    public void setTcpComponent(TCPComponent tcpComponent) {
        this.tcpComponent = tcpComponent;
    }

    public void addOperation(Operation operation) {
        OperationFuture operationFuture = new OperationFuture();
        operation.setFuture(operationFuture);
        pendingQueue.offer(operation);
        //如果队列为空直接写请求，否则加入write队列
        if (writeQueue.size() == 0) {
            directWriteOperation(operation);
        } else {
            writeQueue.add(operation);
            tcpComponent.registerWrite(this);
        }

    }

    private void directWriteOperation(Operation operation) {
        try {
            writeLock.lock();
            boolean full;
            do {
                full = operation.fillWriteBuf(wbuf);
                wbuf.flip();
                socketChannel.write(wbuf);
                if(wbuf.hasRemaining())
                    wbuf.compact();
                else
                    wbuf.clear();
            } while (!full);
        } catch (IOException e) {
            LOGGER.error("write from user error");
        } finally {
            writeLock.unlock();
        }
    }

    public ByteBuffer getWriteByteBuffer() {
        return wbuf;
    }

    public Operation pollWriteCurrentOperation() {
        return writeQueue.poll();
    }

    public Operation peekWriteCurrentOperation() {
        return writeQueue.peek();
    }

    public Operation pollPendingCurrentOperation() {
        return pendingQueue.poll();
    }

    public Operation peekPendingCurrentOperation() {
        return pendingQueue.peek();
    }

    public int getWriteQueueSize() {
        return writeQueue.size();
    }

    public ByteBuffer getRbuf() {
        return rbuf;
    }

    public ReentrantLock getWriteLock() {
        return writeLock;
    }

    public void setWriteLock(ReentrantLock writeLock) {
        this.writeLock = writeLock;
    }
}

package com.sohu.redis.net;

import com.sohu.redis.operation.Operation;
import com.sohu.redis.operation.OperationFuture;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class RedisConnection {

    private LinkedBlockingQueue<Operation> pendingQueue=new LinkedBlockingQueue<Operation>();

    private LinkedBlockingQueue<Operation> writeQueue=new LinkedBlockingQueue<Operation>();

    private ByteBuffer rbuf;

    private ByteBuffer wbuf;

    private String host;

    private int port;

    private SocketChannel socketChannel;

    private TCPComponent tcpComponent;

    private int rBufSize=4096;

    private int wBufSize=4096;

    public RedisConnection(String host,int port){
        this.host=host;
        this.port=port;
        rbuf=ByteBuffer.allocate(rBufSize);
        wbuf=ByteBuffer.allocate(wBufSize);
    }

    public int getrBufSize(){
        return rBufSize;
    }

    public void setrBufSize(int size){
        this.rBufSize=size;
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

    public void addOperation(Operation operation){
        OperationFuture operationFuture=new OperationFuture();
        operation.setFuture(operationFuture);
        pendingQueue.offer(operation);
        writeQueue.add(operation);
        tcpComponent.registerWrite(this);
    }

    public ByteBuffer getWriteByteBuffer(){
        return wbuf;
    }

    public Operation pollWriteCurrentOperation(){
        return writeQueue.poll();
    }

    public Operation peekWriteCurrentOperation(){
        return writeQueue.peek();
    }

    public Operation pollPendingCurrentOperation(){
        return pendingQueue.poll();
    }

    public Operation peekPendingCurrentOperation(){
        return pendingQueue.peek();
    }

    public int getWriteQueueSize(){
        return writeQueue.size();
    }

    public ByteBuffer getRbuf() {
        return rbuf;
    }
}

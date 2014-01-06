package com.sohu.redis.net;

import com.sohu.redis.operation.Operation;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by jianjundeng on 1/6/14.
 */
public interface Connection {

    public void addOperation(Operation operation);

    public void setSocketChannel(SocketChannel socketChannel);

    public void setTcpComponent(TCPComponent tcpComponent);

    public String getHost();

    public int getPort();

    public SocketChannel getSocketChannel();

    public ByteBuffer getWriteByteBuffer();

    public ByteBuffer getReadByteBuffer();

    public void handleRead();

    public void handleWrite();
}

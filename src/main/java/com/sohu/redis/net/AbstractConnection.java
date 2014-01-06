package com.sohu.redis.net;

import com.sohu.redis.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by jianjundeng on 1/6/14.
 */
public abstract class AbstractConnection implements Connection {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnection.class);

    protected ByteBuffer wbuf;

    protected ByteBuffer rbuf;

    protected String host;

    protected int port;

    protected SocketChannel socketChannel;

    protected TCPComponent tcpComponent;

    private int rBufSize = 8*1024;

    private int wBufSize = 8*1024;

    public AbstractConnection(String host, int port){
        this.host = host;
        this.port = port;
        rbuf = ByteBuffer.allocateDirect(rBufSize);
        wbuf = ByteBuffer.allocateDirect(wBufSize);
    }

    @Override
    public abstract void addOperation(Operation operation);

    @Override
    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel=socketChannel;
    }

    @Override
    public void setTcpComponent(TCPComponent tcpComponent) {
        this.tcpComponent=tcpComponent;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    protected void directWriteOperation(Operation operation) {
        try {
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
            directWriteAfter();
        }
    }

    public void directWriteAfter(){

    }

    public ByteBuffer getWriteByteBuffer(){
        return wbuf;
    }

    public ByteBuffer getReadByteBuffer(){
        return rbuf;
    }

}

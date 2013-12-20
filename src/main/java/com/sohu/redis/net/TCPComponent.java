package com.sohu.redis.net;

import com.sohu.redis.operation.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class TCPComponent extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCPComponent.class);

    private Selector selector;

    private LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<Event>();

    public TCPComponent() {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            LOGGER.error("selector open error", e);
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (events.size() > 0)
                    processEvents();
                int count = selector.select(1000l);
                if (count == 0)
                    continue;
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isReadable()) {
                        handlerRead(key);
                    } else if (key.isWritable()) {
                        handlerWrite(key);
                    }
                    iter.remove();
                }
            } catch (IOException e) {
                LOGGER.error("select error");
            }
        }
    }

    private void processEvents() {
        try {
            Event event;
            while ((event = events.poll()) != null) {
                switch (event.getEventType()) {
                    case WRITE:
                        event.getRedisConnection().getSocketChannel().register(selector, SelectionKey.OP_WRITE, event.getRedisConnection());
                }
            }
        } catch (ClosedChannelException e) {
            LOGGER.error("process event error");
        }
    }

    private void handlerWrite(SelectionKey key) {

        RedisConnection connection = (RedisConnection) key.attachment();
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer byteBuffer=connection.getWriteByteBuffer();
        Operation operation;
        while ((operation=connection.peekWriteCurrentOperation()) != null) {
            boolean isAll=operation.fillWriteBuf(byteBuffer);
            try {
                if(isAll){
                    connection.pollWriteCurrentOperation();
                    if(byteBuffer.hasRemaining()&&connection.peekWriteCurrentOperation()!=null){
                        continue;
                    }
                }
                byteBuffer.flip();
                socketChannel.write(byteBuffer);
                if(byteBuffer.hasRemaining()){
                    byteBuffer.compact();
                }else{
                    byteBuffer.clear();
                }
            } catch (IOException e) {
                LOGGER.error("write error");
            }
        }
        if(!key.isReadable()){
            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        }

        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    private void handlerRead(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        RedisConnection connection = (RedisConnection) key.attachment();
        ByteBuffer rbuf = connection.getRbuf();
        int count;
        while (true) {
            try {
                count = socketChannel.read(rbuf);
                if (count > 0) {
                    rbuf.flip();
                    while (rbuf.hasRemaining()) {
                        Operation operation = connection.peekPendingCurrentOperation();
                        boolean result = operation.completeData(rbuf);
                        if (result) {
                            connection.pollPendingCurrentOperation();
                            operation.pushData();
                        }
                    }
                    rbuf.clear();
                } else {
                    break;
                }
            } catch (IOException e) {
                LOGGER.error("read error");
            }
        }
    }

    public void registerWrite(RedisConnection connection) {
        events.add(new Event(EventType.WRITE, connection));
        selector.wakeup();
    }

    public void register(RedisConnection connection) {
        try {
            SocketChannel channel = SocketChannel.open();
            connection.setSocketChannel(channel);
            connection.setTcpComponent(this);
            boolean connect = channel.connect(new InetSocketAddress(connection.getHost(), connection.getPort()));
            if (!connect) {
                LOGGER.error("connect error");
            } else {
                channel.configureBlocking(false);
            }
        } catch (IOException e) {
            LOGGER.error("socket channel open error", e);
        }
    }
}

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
                if (events.size() > 0){
                    processEvents();
                }
                int count = selector.select(1000l);
                if (count == 0)
                    continue;
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isReadable() && key.isValid()) {
                        handlerRead(key);
                    } else if (key.isWritable() && key.isValid()) {
                        handlerWrite(key);
                    } else if (key.isConnectable() && key.isValid()) {
                        handlerConnect(key);
                    }
                    iter.remove();
                }
            } catch (IOException e) {
                LOGGER.error("select error");
            }
        }
    }

    private void handlerConnect(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        try {
            boolean connect = socketChannel.finishConnect();
            if (connect) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
                key.interestOps(key.interestOps()|SelectionKey.OP_READ);
            }
        } catch (IOException e) {
            LOGGER.error("finish connect error");
        }
    }

    private void processEvents() {
        try {
            Event event;
            SelectionKey key;
            while ((event = events.poll()) != null) {
                SocketChannel channel=event.getConnection().getSocketChannel();
                key=channel.keyFor(selector);
                switch (event.getEventType()) {
                    case CONNECT:
                        channel.register(selector, SelectionKey.OP_CONNECT, event.getConnection());
                        break;
                    case WRITE:
                        if((key.interestOps()|SelectionKey.OP_WRITE)!=0){
                           key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                        break;
                    case READ:
                        if(key==null){
                            channel.register(selector,SelectionKey.OP_READ,event.getConnection());
                        }else if((key.interestOps()|SelectionKey.OP_READ)!=0){
                            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                        }
                        break;
                    case UNWRITE:
                        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        break;
                }
            }
        } catch (ClosedChannelException e) {
            LOGGER.error("process event error");
        }
    }

    private void handlerWrite(SelectionKey key) {
       Connection connection = (Connection) key.attachment();
       connection.handleWrite();
    }

    private void handlerRead(SelectionKey key) {
        Connection connection = (Connection) key.attachment();
        connection.handleRead();
    }

    public void registerWrite(Connection connection) {
        events.add(new Event(EventType.WRITE, connection));
        selector.wakeup();
    }

    public void registerConnect(Connection connection) {
        events.add(new Event(EventType.CONNECT, connection));
        selector.wakeup();
    }

    public void registerRead(Connection connection) {
        events.add(new Event(EventType.READ, connection));
        selector.wakeup();
    }

    public void registerUnWrite(Connection connection){
        events.add(new Event(EventType.UNWRITE,connection));
        selector.wakeup();
    }

    public void register(Connection connection) {
        try {
            SocketChannel channel = SocketChannel.open();
            connection.setSocketChannel(channel);
            connection.setTcpComponent(this);
            channel.configureBlocking(false);
            boolean connect = channel.connect(new InetSocketAddress(connection.getHost(), connection.getPort()));
            if (!connect) {
                registerConnect(connection);
            }else{
                //开始就会关注读事件
                registerRead(connection);
            }
        } catch (IOException e) {
            LOGGER.error("socket channel open error", e);
        }
    }
}

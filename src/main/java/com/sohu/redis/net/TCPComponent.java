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
                    if (key.isReadable()&&key.isValid()){
                        handlerRead(key);
                    }else if(key.isWritable()&&key.isValid()){
                        handlerWrite(key);
                    }else if(key.isConnectable()&&key.isValid()){
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
        SocketChannel socketChannel = (SocketChannel)key.channel();
        try {
            boolean connect=socketChannel.finishConnect();
            if(connect){
                key.interestOps(key.interestOps()&~SelectionKey.OP_CONNECT);
            }
        } catch (IOException e) {
            LOGGER.error("finish connect error");
        }
    }

    private void processEvents() {
        try {
            Event event;
            while ((event = events.poll()) != null) {
                switch (event.getEventType()) {
                    case WRITE:
                        SelectionKey key=event.getRedisConnection().getSocketChannel().keyFor(selector);
                        key.interestOps(key.interestOps()|SelectionKey.OP_WRITE);
                        break;
                    case CONNECT:
                        event.getRedisConnection().getSocketChannel().register(selector,SelectionKey.OP_CONNECT,event.getRedisConnection());
                        break;
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
        //从写队列中拿出operation
        while ((operation=connection.peekWriteCurrentOperation()) != null) {
            //填充operation的数据到byte buffer中，返回是否填充完毕，如果byte buffer不够大，就返回false
            boolean isAll=operation.fillWriteBuf(byteBuffer);
            try {
                if(isAll){
                    //如果填充完毕，就从写队列中移除operation
                    connection.pollWriteCurrentOperation();
                    //如果byte buffer 还有容量，并且还有其他的operation需要写数据，那么继续选择其他后面的operation填充byte buffer
                    if(byteBuffer.hasRemaining()&&connection.peekWriteCurrentOperation()!=null){
                        continue;
                    }
                }
                //这里表明，byte buffer满了，或者没有其他的operation需要填充了，那么就可以写数据了
                byteBuffer.flip();
                socketChannel.write(byteBuffer);

                if(byteBuffer.hasRemaining()){
                    //如果byte buffer中的数据未全写完，那么返回，等待write事件，同时回收byte buffer
                    byteBuffer.compact();
                    return;
                }else{
                    //byte buffer全部写完，清空byte buffer
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
                    //byte buffer中有数据，就从pending队列中取出operation，pipeline
                    while (rbuf.hasRemaining()) {
                        Operation operation = connection.peekPendingCurrentOperation();
                        boolean result = operation.completeData(rbuf);
                        if (result) {
                            //operation解析byte buffer中的数据完整了，就移除operation，回调客户端线程
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

    public void registerConnect(RedisConnection connection){
        events.add(new Event(EventType.CONNECT,connection));
        selector.wakeup();
    }

    public void register(RedisConnection connection) {
        try {
            SocketChannel channel = SocketChannel.open();
            connection.setSocketChannel(channel);
            connection.setTcpComponent(this);
            channel.configureBlocking(false);
            boolean connect = channel.connect(new InetSocketAddress(connection.getHost(), connection.getPort()));
            if (!connect) {
                registerConnect(connection);
            }
        } catch (IOException e) {
            LOGGER.error("socket channel open error", e);
        }
    }
}

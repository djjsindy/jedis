package com.sohu.redis.net;

import com.sohu.redis.operation.Operation;
import com.sohu.redis.operation.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class RedisConnection extends AbstractConnection{

    private LinkedBlockingQueue<Operation> pendingQueue = new LinkedBlockingQueue<Operation>();

    private LinkedBlockingQueue<Operation> writeQueue = new LinkedBlockingQueue<Operation>();

    private ReentrantLock writeLock = new ReentrantLock();

    public RedisConnection(String host, int port) {
        super(host, port);
    }


    public void addOperation(Operation operation) {
        OperationFuture operationFuture = new OperationFuture();
        operation.setFuture(operationFuture);
        pendingQueue.offer(operation);
        //如果队列为空直接写请求，并且请求write lock，成功direct write，否则加入write队列
        //加锁失败由于tcpComponent线程在写缓冲队列中的数据，这个过程不应该被打断。
        if (writeQueue.size() == 0&&writeLock.tryLock()) {
            directWriteOperation(operation);
        } else {
            writeQueue.add(operation);
            tcpComponent.registerWrite(this);
        }

    }

    public void directWriteAfter(){
        writeLock.unlock();
    }

    /**
     * 读取数据的回调方法
     */
    public void handleRead() {
        int count;
        while (true) {
            try {
                count = socketChannel.read(rbuf);
                if (count > 0) {
                    rbuf.flip();
                    //byte buffer中有数据，就从pending队列中取出operation，pipeline
                    while (rbuf.hasRemaining()) {
                        Operation operation = pendingQueue.peek();
                        boolean result = operation.completeData(rbuf);
                        if (result) {
                            //operation解析byte buffer中的数据完整了，就移除operation，回调客户端线程
                            pendingQueue.poll();
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

    /**
     * 写操作的回调方法
     */
    public void handleWrite(){
        Operation operation;
        try {
            writeLock.lock();
            //判断如果当前没有operation了，那么有可能最后一次write没有写完，后面的逻辑不可能再去触发写了
            //这里把最后的buffer写出去，如果后面还有operation，继续fill command就行了
            if (writeQueue.peek() == null) {
                wbuf.flip();
                if (wbuf.hasRemaining()) {
                    socketChannel.write(wbuf);
                    if (wbuf.hasRemaining()) {
                        wbuf.compact();
                        return;
                    }
                }
            }
            //从写队列中拿出operation
            while ((operation = writeQueue.peek()) != null) {
                //填充operation的数据到byte buffer中，返回是否填充完毕，如果byte buffer不够大，就返回false
                boolean isAll = operation.fillWriteBuf(wbuf);
                if (isAll) {
                    //如果填充完毕，就从写队列中移除operation
                    writeQueue.poll();
                    //如果byte buffer 还有容量，并且还有其他的operation需要写数据，那么继续选择其他后面的operation填充byte buffer
                    if (wbuf.hasRemaining() && writeQueue.peek() != null) {
                        continue;
                    }
                }
                //这里表明，byte buffer满了，或者没有其他的operation需要填充了，那么就可以写数据了
                wbuf.flip();
                socketChannel.write(wbuf);
                if (wbuf.hasRemaining()) {
                    //如果byte buffer中的数据未全写完，那么返回，等待write事件，同时回收byte buffer
                    wbuf.compact();
                    return;
                } else {
                    //byte buffer全部写完，清空byte buffer
                    wbuf.clear();
                }

            }

        } catch (IOException e) {
            LOGGER.error("write error");
        }
        //最后如果write 队列为空了证明了数据都写了，释放写锁，为了请求直接在add operation中直接写数据
        if(writeQueue.size()==0){
            writeLock.unlock();
        }
        tcpComponent.registerUnWrite(this);
    }
}

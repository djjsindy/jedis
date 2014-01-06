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
        //�������Ϊ��ֱ��д���󣬲�������write lock���ɹ�direct write���������write����
        //����ʧ������tcpComponent�߳���д��������е����ݣ�������̲�Ӧ�ñ���ϡ�
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
     * ��ȡ���ݵĻص�����
     */
    public void handleRead() {
        int count;
        while (true) {
            try {
                count = socketChannel.read(rbuf);
                if (count > 0) {
                    rbuf.flip();
                    //byte buffer�������ݣ��ʹ�pending������ȡ��operation��pipeline
                    while (rbuf.hasRemaining()) {
                        Operation operation = pendingQueue.peek();
                        boolean result = operation.completeData(rbuf);
                        if (result) {
                            //operation����byte buffer�е����������ˣ����Ƴ�operation���ص��ͻ����߳�
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
     * д�����Ļص�����
     */
    public void handleWrite(){
        Operation operation;
        try {
            writeLock.lock();
            //�ж������ǰû��operation�ˣ���ô�п������һ��writeû��д�꣬������߼���������ȥ����д��
            //���������bufferд��ȥ��������滹��operation������fill command������
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
            //��д�������ó�operation
            while ((operation = writeQueue.peek()) != null) {
                //���operation�����ݵ�byte buffer�У������Ƿ������ϣ����byte buffer�����󣬾ͷ���false
                boolean isAll = operation.fillWriteBuf(wbuf);
                if (isAll) {
                    //��������ϣ��ʹ�д�������Ƴ�operation
                    writeQueue.poll();
                    //���byte buffer �������������һ���������operation��Ҫд���ݣ���ô����ѡ�����������operation���byte buffer
                    if (wbuf.hasRemaining() && writeQueue.peek() != null) {
                        continue;
                    }
                }
                //���������byte buffer���ˣ�����û��������operation��Ҫ����ˣ���ô�Ϳ���д������
                wbuf.flip();
                socketChannel.write(wbuf);
                if (wbuf.hasRemaining()) {
                    //���byte buffer�е�����δȫд�꣬��ô���أ��ȴ�write�¼���ͬʱ����byte buffer
                    wbuf.compact();
                    return;
                } else {
                    //byte bufferȫ��д�꣬���byte buffer
                    wbuf.clear();
                }

            }

        } catch (IOException e) {
            LOGGER.error("write error");
        }
        //������write ����Ϊ����֤�������ݶ�д�ˣ��ͷ�д����Ϊ������ֱ����add operation��ֱ��д����
        if(writeQueue.size()==0){
            writeLock.unlock();
        }
        tcpComponent.registerUnWrite(this);
    }
}

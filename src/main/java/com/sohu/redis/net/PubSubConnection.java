package com.sohu.redis.net;

import com.sohu.redis.model.PubSubCallBack;
import com.sohu.redis.operation.Operation;
import com.sohu.redis.operation.Response;
import com.sohu.redis.protocol.RedisProtocol;

import java.io.IOException;

/**
 * Created by jianjundeng on 1/6/14.
 */
public class PubSubConnection extends AbstractConnection{

    private PubSubCallBack pubSubCallBack;

    private Operation operation;

    private Response msgResponse=new Response();

    public PubSubConnection(String host, int port) {
        super(host, port);
    }

    @Override
    public void addOperation(Operation operation) {
        this.operation=operation;
        directWriteOperation(operation);
    }

    @Override
    public void handleRead() {
        int count;
        while (true) {
            try {
                count = socketChannel.read(rbuf);
                if (count > 0) {
                    rbuf.flip();
                    //byte buffer中有数据，就从pending队列中取出operation，pipeline
                    while (rbuf.hasRemaining()) {
                        if(!operation.isFinish()){
                            boolean result = operation.completeData(rbuf);
                            if (result) {
                                //operation解析byte buffer中的数据完整了，回调客户端线程
                                operation.pushData();
                            }else{
                                return;
                            }
                        }else{
                            boolean result=RedisProtocol.processResult(rbuf, msgResponse);
                            if(result){
                                RedisProtocol.callback(msgResponse,pubSubCallBack);
                                msgResponse.clear();
                            }else{
                                return;
                            }
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

    @Override
    public void handleWrite() {
       //nothing to do
    }

    public PubSubCallBack getPubSubCallBack() {
        return pubSubCallBack;
    }

    public void setPubSubCallBack(PubSubCallBack pubSubCallBack) {
        this.pubSubCallBack = pubSubCallBack;
    }
}

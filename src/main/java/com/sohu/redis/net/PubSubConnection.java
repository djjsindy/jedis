package com.sohu.redis.net;

import com.sohu.redis.model.PubSubCallBack;
import com.sohu.redis.operation.Operation;
import com.sohu.redis.operation.Response;
import com.sohu.redis.protocol.RedisProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianjundeng on 1/6/14.
 */
public class PubSubConnection extends AbstractConnection {

    private Map<String,PubSubCallBack> callBackMap;

    private Response msgResponse = new Response();

    public PubSubConnection(String host, int port) {
        super(host, port);
        callBackMap=new HashMap<String, PubSubCallBack>();
    }

    @Override
    public void addOperation(Operation operation) {
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
                        boolean result = RedisProtocol.processResult(rbuf, msgResponse);
                        if (result) {
                            RedisProtocol.callback(msgResponse,this);
                            msgResponse.clear();
                        } else {
                            return;
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

    public void addCallBack(String channel,PubSubCallBack pubSubCallBack){
        callBackMap.put(channel, pubSubCallBack);
    }

    public void removeCallBack(String channel){
        callBackMap.remove(channel);
    }

    public Map<String,PubSubCallBack> getCallBackMap(){
        return callBackMap;
    }
}

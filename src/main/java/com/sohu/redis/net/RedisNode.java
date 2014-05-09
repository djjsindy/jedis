package com.sohu.redis.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class RedisNode {

    private List<RedisConnection> connections;

    private int conNum=1;

    private static TCPComponent tcpComponent=new TCPComponent();

    private Random random=new Random();

    private PubSubConnection pubSubConnection;

    static{
        tcpComponent.start();
    }

    public RedisNode(String host,int port){
        //connect
        connections=new ArrayList<RedisConnection>(conNum);
        for(int i=0;i<conNum;i++){
            RedisConnection connection=new RedisConnection(host,port);
            connections.add(connection);
            tcpComponent.register(connection);
        }
        pubSubConnection=new PubSubConnection(host,port);
        tcpComponent.register(pubSubConnection);
    }

    public RedisConnection getAvailableConnection(){
        return connections.get(random.nextInt(connections.size()));
    }

    public PubSubConnection getPubSubConnection(){
        return pubSubConnection;
    }


}

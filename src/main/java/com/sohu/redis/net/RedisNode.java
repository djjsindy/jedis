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

    private String host;

    private int port;

    public RedisNode(String host,int port){
        this.host=host;
        this.port=port;
        tcpComponent.start();
        //connect
        connections=new ArrayList<RedisConnection>(conNum);
        for(int i=0;i<conNum;i++){
            connections.add(new RedisConnection(host,port));
        }
        for(RedisConnection connection:connections)
            tcpComponent.register(connection);
    }

    public RedisConnection getAvailableConnection(){
        return connections.get(random.nextInt(connections.size()));
    }

    public PubSubConnection getPubSubConnection(){
        PubSubConnection pubSubConnection=new PubSubConnection(host,port);
        tcpComponent.register(pubSubConnection);
        return pubSubConnection;
    }


}

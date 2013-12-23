package com.sohu.redis;

import com.sohu.redis.net.RedisNode;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class NodeSelector {

    private RedisNode redisNodes[];

    public NodeSelector(String cons){
        String[] con=cons.split(",");
        redisNodes=new RedisNode[con.length];
        int i=0;
        for(String connectStr:con){
            String part[]=connectStr.split(":");
            redisNodes[i]=new RedisNode(part[0],Integer.parseInt(part[1]));
        }
    }

    public RedisNode getNodeByKey(String key){
        return redisNodes[key.hashCode()%redisNodes.length];
    }

    public List<RedisNode> getAllNodes(){
        return Arrays.asList(redisNodes);
    }
}

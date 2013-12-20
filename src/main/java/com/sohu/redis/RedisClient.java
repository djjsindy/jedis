package com.sohu.redis;

import com.sohu.redis.net.RedisConnection;
import com.sohu.redis.net.RedisNode;
import com.sohu.redis.operation.Operation;
import com.sohu.redis.transform.Serializer;
import com.sohu.redis.transform.SimpleSerializer;
import com.sohu.redis.transform.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);

    private NodeSelector nodeSelector;

    private Serializer serializer=new SimpleSerializer();

    public RedisClient(String cons) {
        nodeSelector = new NodeSelector(cons);
    }

    /**
     * get string
     * @param key
     * @return
     */
    public String getString(String key) {
        RedisConnection connection=getConnection(key);
        try {
            Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
            connection.addOperation(operation);
            byte[] data = ((byte[][]) operation.getFuture().get())[0];
            if(data==null)
                return null;
            return StringEncoder.getString(data);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    /**
     * get Object
     * @param key
     * @return
     */
    public Object getObject(String key) {
        RedisConnection connection=getConnection(key);
        try {
            Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
            connection.addOperation(operation);
            byte[] data = (byte[]) operation.getFuture().get();
            if(data==null)
                return null;
            return serializer.decode(data);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    /**
     * set string
     * @param key
     * @param seconds
     * @param value
     * @return
     */
    public String setString(String key,int seconds,String value) {
        RedisConnection connection=getConnection(key);
        try {
            Operation operation;
            if(seconds>0){
                operation = new Operation(Operation.Command.SETEX, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(seconds)),StringEncoder.getBytes(value));
            }else{
                operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key),StringEncoder.getBytes(value));
            }

            connection.addOperation(operation);
            byte[] data = ((byte[][]) operation.getFuture().get())[0];
            return StringEncoder.getString(data);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    public String setObject(String key,int seconds,Object object) {
        RedisConnection connection=getConnection(key);
        try {
            Operation operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(seconds)),serializer.encode(object));
            connection.addOperation(operation);
            byte[] data = (byte[]) operation.getFuture().get();
            return StringEncoder.getString(data);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    private RedisConnection getConnection(String key){
        RedisNode redisNode = nodeSelector.getNodeByKey(key);
        RedisConnection connection = redisNode.getAvailableConnection();
        return connection;
    }
}

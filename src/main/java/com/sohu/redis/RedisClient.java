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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class RedisClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);

    private NodeSelector nodeSelector;

    private static final int TIMEOUT = 5;

    private Serializer serializer = new SimpleSerializer();

    public RedisClient(String cons) {
        nodeSelector = new NodeSelector(cons);
    }


    /**
     * get string
     *
     * @param key
     * @return
     */
    public String getString(String key) {
        Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return response != null ? StringEncoder.getString(response) : null;
    }

    /**
     * get Object
     *
     * @param key
     * @return
     */
    public Object getObject(String key) {
        Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return response != null ? serializer.decode(response) : null;
    }

    /**
     * set string
     *
     * @param key
     * @param seconds
     * @param value
     * @return
     */
    public String setString(String key, int seconds, String value) {
        Operation operation;
        if (seconds > 0) {
            operation = new Operation(Operation.Command.SETEX, StringEncoder.getBytes(key), StringEncoder.getBytes(String.valueOf(seconds)), StringEncoder.getBytes(value));
        } else {
            operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key), StringEncoder.getBytes(value));
        }
        byte[] response = singleKeyRequest(operation, key);
        return response != null ? StringEncoder.getString(response) : null;
    }

    public String setObject(String key, int seconds, Object object) {
        Operation operation;
        if (seconds > 0) {
            operation = new Operation(Operation.Command.SETEX, StringEncoder.getBytes(key), StringEncoder.getBytes(String.valueOf(seconds)), serializer.encode(object));
        } else {
            operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key), serializer.encode(object));
        }
        byte[] response = singleKeyRequest(operation, key);
        return response != null ? StringEncoder.getString(response) : null;
    }

    public boolean exists(final String key) {
        Operation operation = new Operation(Operation.Command.EXISTS, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return response==null?false:(response[0]==49?true:false);
    }

    public boolean del(final String key){
        Operation operation = new Operation(Operation.Command.DEL, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return response==null?false:(response[0]==49?true:false);
    }

    public boolean expire(String key, int expireSeconds) {
        Operation operation = new Operation(Operation.Command.EXPIRE, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(expireSeconds)));
        byte[] response = singleKeyRequest(operation, key);
        return response==null?false:(response[0]==49?true:false);
    }

    public Long incr(String key) {
        Operation operation = new Operation(Operation.Command.INCR, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return buildLong(response);
    }

    public Long incrBy(String key,long step){
        Operation operation = new Operation(Operation.Command.INCRBY, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(step)));
        byte[] response = singleKeyRequest(operation, key);
        return buildLong(response);
    }

    public Long decr(String key){
        Operation operation = new Operation(Operation.Command.DECR, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return buildLong(response);
    }

    public Long decrBy(String key,long step){
        Operation operation = new Operation(Operation.Command.DECRBY, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(step)));
        byte[] response = singleKeyRequest(operation, key);
        return buildLong(response);
    }

    private Long buildLong(byte[] response) {
        StringBuilder sb=new StringBuilder();
        for(byte b:response){
            sb.append((char)b);
        }
        return Long.valueOf(sb.toString());
    }


    private RedisConnection getConnection(String key) {
        RedisNode redisNode = nodeSelector.getNodeByKey(key);
        RedisConnection connection = redisNode.getAvailableConnection();
        return connection;
    }

    private byte[] singleKeyRequest(Operation operation, String key) {
        try {
            RedisConnection connection = getConnection(key);
            connection.addOperation(operation);
            byte[] data = ((byte[][]) operation.getFuture().get(TIMEOUT, TimeUnit.SECONDS))[0];
            return data;
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage());
        } catch (TimeoutException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }
}

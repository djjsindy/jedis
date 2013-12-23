package com.sohu.redis;

import com.sohu.redis.net.RedisConnection;
import com.sohu.redis.net.RedisNode;
import com.sohu.redis.operation.Operation;
import com.sohu.redis.transform.Serializer;
import com.sohu.redis.transform.SimpleSerializer;
import com.sohu.redis.transform.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

    public String setByteArr(String key, int seconds, byte[] value) {
        Operation operation;
        if (seconds > 0) {
            operation = new Operation(Operation.Command.SETEX, StringEncoder.getBytes(key), StringEncoder.getBytes(String.valueOf(seconds)), value);
        } else {
            operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key), value);
        }
        byte[] response = singleKeyRequest(operation, key);
        return response != null ? StringEncoder.getString(response) : null;
    }

    public byte[] getByteArr(String key){
        Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
        return singleKeyRequest(operation, key);
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

    public List<String> flushAll(){
        Operation operation=new Operation(Operation.Command.FLUSHALL);
        List<byte []>result=controlRequest(operation);
        return StringEncoder.getStringList(result);
    }

    public Set<String> keys(final String pattern) {
        Operation operation=new Operation(Operation.Command.KEYS,StringEncoder.getBytes(pattern));
        List<byte []>result=controlRequest(operation);
        return StringEncoder.getStringSet(result);
    }

    public Long ttl(final String key) {
        Operation operation = new Operation(Operation.Command.TTL, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key);
        return buildLong(response);
    }

    public String getSet(final String key, final String value) {
        Operation operation = new Operation(Operation.Command.GETSET, StringEncoder.getBytes(key),StringEncoder.getBytes(value));
        byte[] response = singleKeyRequest(operation, key);
        return response != null ? StringEncoder.getString(response) : null;
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

    private List<RedisConnection> getAllAcailableConnections(){
        List<RedisNode> nodes=nodeSelector.getAllNodes();
        List<RedisConnection> connections=new ArrayList<RedisConnection>();
        for(RedisNode redisNode:nodes)
            connections.add(redisNode.getAvailableConnection());
        return connections;
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

    private List<byte[]> controlRequest(Operation operation){
        List<byte[]> result=new ArrayList<byte[]>();
        for(RedisConnection redisConnection:getAllAcailableConnections()){
            try {
                redisConnection.addOperation(operation);
                result.add(((byte[][]) operation.getFuture().get(TIMEOUT, TimeUnit.SECONDS))[0]);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
            } catch (ExecutionException e) {
                LOGGER.error(e.getMessage());
            } catch (TimeoutException e) {
                LOGGER.error(e.getMessage());
            }
        }
        return result;
    }
}

package com.sohu.redis;

import com.sohu.redis.model.Pair;
import com.sohu.redis.net.RedisConnection;
import com.sohu.redis.net.RedisNode;
import com.sohu.redis.operation.Operation;
import com.sohu.redis.transform.Serializer;
import com.sohu.redis.transform.SimpleSerializer;
import com.sohu.redis.transform.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0? StringEncoder.getString(response) : null;
    }

    /**
     * get Object
     *
     * @param key
     * @return
     */
    public Object getObject(String key) {
        Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0 ? serializer.decode(response) : null;
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
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0 ? StringEncoder.getString(response) : null;
    }

    public String setObject(String key, int seconds, Object object) {
        Operation operation;
        if (seconds > 0) {
            operation = new Operation(Operation.Command.SETEX, StringEncoder.getBytes(key), StringEncoder.getBytes(String.valueOf(seconds)), serializer.encode(object));
        } else {
            operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key), serializer.encode(object));
        }
        byte[] response = singleKeyRequest(operation, key)[0];
        return response != null ? StringEncoder.getString(response) : null;
    }

    public String setByteArr(String key, int seconds, byte[] value) {
        Operation operation;
        if (seconds > 0) {
            operation = new Operation(Operation.Command.SETEX, StringEncoder.getBytes(key), StringEncoder.getBytes(String.valueOf(seconds)), value);
        } else {
            operation = new Operation(Operation.Command.SET, StringEncoder.getBytes(key), value);
        }
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0 ? StringEncoder.getString(response) : null;
    }

    public byte[] getByteArr(String key){
        Operation operation = new Operation(Operation.Command.GET, StringEncoder.getBytes(key));
        return singleKeyRequest(operation, key)[0];
    }

    public boolean exists(final String key) {
        Operation operation = new Operation(Operation.Command.EXISTS, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0?false:(response[0]==49?true:false);
    }

    public boolean del(final String key){
        Operation operation = new Operation(Operation.Command.DEL, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0?false:(response[0]==49?true:false);
    }

    public boolean expire(String key, int expireSeconds) {
        Operation operation = new Operation(Operation.Command.EXPIRE, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(expireSeconds)));
        byte[] response = singleKeyRequest(operation, key)[0];
        return response.length!=0?false:(response[0]==49?true:false);
    }

    public Long incr(String key) {
        Operation operation = new Operation(Operation.Command.INCR, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public Long incrBy(String key,long step){
        Operation operation = new Operation(Operation.Command.INCRBY, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(step)));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public Long decr(String key){
        Operation operation = new Operation(Operation.Command.DECR, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public Long decrBy(String key,long step){
        Operation operation = new Operation(Operation.Command.DECRBY, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(step)));
        byte[] response = singleKeyRequest(operation, key)[0];
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
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public String getSet(final String key, final String value) {
        Operation operation = new Operation(Operation.Command.GETSET, StringEncoder.getBytes(key),StringEncoder.getBytes(value));
        byte[] response = singleKeyRequest(operation, key)[0];
        return response != null ? StringEncoder.getString(response) : null;
    }

    public List<String> mget(final String... keys) {
        List<byte[]> response=multiKeyRequest(Operation.Command.MGET,keys,null);
        return StringEncoder.getStringList(response);
    }

    public Long setnx(final String key, final String value) {
        Operation operation = new Operation(Operation.Command.SETNX, StringEncoder.getBytes(key),StringEncoder.getBytes(value));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public List<String> mset(final String... keysvalues) {
        if(keysvalues.length%2!=0){
            LOGGER.error("key value length error");
            return null;
        }
        String[] keys=new String[keysvalues.length/2];
        String[] values=new String[keysvalues.length/2];
        int index=0;
        for(String str:keysvalues){
            if(index%2==0){
                keys[index/2]=str;
            }else{
                values[index/2]=str;
            }
        }
        List<byte[]> response=multiKeyRequest(Operation.Command.MGET,keys,values);
        return StringEncoder.getStringList(response);
    }

    public Long append(String key,String value) {
        Operation operation = new Operation(Operation.Command.APPEND, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(value)));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public String substr(String key, int start, int end) {
        Operation operation = new Operation(Operation.Command.SUBSTR, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(start)),StringEncoder.getBytes(String.valueOf(end)));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }

    public Long rpush(String key,String str) {
        Operation operation = new Operation(Operation.Command.RPUSH, StringEncoder.getBytes(key),StringEncoder.getBytes(str));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }


    public Long lpush(String key,String str) {
        Operation operation = new Operation(Operation.Command.LPUSH, StringEncoder.getBytes(key),StringEncoder.getBytes(str));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public Long llen(String key) {
        Operation operation = new Operation(Operation.Command.LLEN, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }


    public List<String> lrange(String key,long start,long end) {
        Operation operation = new Operation(Operation.Command.LPUSH, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(start)),StringEncoder.getBytes(String.valueOf(end)));
        byte[][] response = singleKeyRequest(operation, key);
        return StringEncoder.getStringList(Arrays.asList(response));
    }


    public String ltrim(String key,long start,long end) {
        Operation operation = new Operation(Operation.Command.LTRIM, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(start)),StringEncoder.getBytes(String.valueOf(end)));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }


    public String lindex(String key,long index) {
        Operation operation = new Operation(Operation.Command.LINDEX, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(index)));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }


    public String lset(String key,long index, String value) {
        Operation operation = new Operation(Operation.Command.LSET, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(index)),StringEncoder.getBytes(value));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }


    public Long lrem(String key,long count, String value) {
        Operation operation = new Operation(Operation.Command.LREM, StringEncoder.getBytes(key),StringEncoder.getBytes(String.valueOf(count)),StringEncoder.getBytes(value));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }


    public String lpop(String key) {
        Operation operation = new Operation(Operation.Command.LPOP, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }


    public String rpop(String key) {
        Operation operation = new Operation(Operation.Command.RPOP, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }

    public Long sadd(String key,String member) {
        Operation operation = new Operation(Operation.Command.SADD, StringEncoder.getBytes(key),StringEncoder.getBytes(member));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public Set<String> smembers(String key) {
        Operation operation = new Operation(Operation.Command.SMEMBERS, StringEncoder.getBytes(key));
        byte[][] response = singleKeyRequest(operation, key);
        return StringEncoder.getStringSet(Arrays.asList(response));
    }

    public Long srem(String key,String member) {
        Operation operation = new Operation(Operation.Command.SREM, StringEncoder.getBytes(key),StringEncoder.getBytes(member));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public String spop(String key) {
        Operation operation = new Operation(Operation.Command.SPOP, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }

    public Long scard(final String key) {
        Operation operation = new Operation(Operation.Command.SCARD, StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response);
    }

    public Boolean sismember(final String key, final String member) {
        Operation operation = new Operation(Operation.Command.SISMEMBER, StringEncoder.getBytes(key),StringEncoder.getBytes(member));
        byte[] response = singleKeyRequest(operation, key)[0];
        return buildLong(response)==1;
    }

    public String srandmember(final String key) {
        Operation operation = new Operation(Operation.Command.SRANDMEMBER,StringEncoder.getBytes(key));
        byte[] response = singleKeyRequest(operation, key)[0];
        return StringEncoder.getString(response);
    }

    private List<byte[]> multiKeyRequest(Operation.Command command, String[] keys,String[] values) {
        List<byte[]> result=new ArrayList<byte[]>();
        //按照key找connection
        Map<RedisConnection,List<Pair>> data=new HashMap<RedisConnection, List<Pair>>();
        int i=0;
        for(String key:keys){
            RedisConnection redisConnection=getConnection(key);
            List<Pair> keyList=data.get(redisConnection);
            if(keyList==null){
                keyList=new ArrayList<Pair>();
                data.put(redisConnection,keyList);
            }
            keyList.add(new Pair(key,values==null?null:values[i]));
            i++;
        }
        //组装multi args operation，加入connection write queue
        List<Operation> operations=new ArrayList<Operation>();
        for(Map.Entry<RedisConnection,List<Pair>> entry:data.entrySet()){
            Operation operation = new Operation(command);
            byte[][] args=new byte[entry.getValue().size()*(values==null?1:2)][];
            int index=0;
            for(Pair pair:entry.getValue()){
                args[index++]=StringEncoder.getBytes(pair.getKey());
                if(values!=null){
                    args[index++]=StringEncoder.getBytes(pair.getValue());
                }
            }
            operation.setArgs(args);
            entry.getKey().addOperation(operation);
            operations.add(operation);
        }
        //future获得结果
        for(Operation operation:operations){
            try {
                result.addAll(Arrays.asList((byte[][]) operation.getFuture().get(TIMEOUT, TimeUnit.SECONDS)));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
        return result;
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

    private byte[][] singleKeyRequest(Operation operation, String key) {
        try {
            RedisConnection connection = getConnection(key);
            connection.addOperation(operation);
            return  ((byte[][]) operation.getFuture().get(TIMEOUT, TimeUnit.SECONDS));
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

package com.sohu.redis.operation;

import com.sohu.redis.protocol.ParseStatus;
import com.sohu.redis.protocol.RedisProtocol;
import com.sohu.redis.protocol.SubParseContext;
import com.sohu.redis.protocol.WritePhase;
import com.sohu.redis.transform.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(Operation.class);

    private OperationFuture future;

    private boolean finish;

    private Request request;

    private Response response;

    public Operation(Command command, byte[]... args) {
        this.request=new Request();
        this.response=new Response();
        this.request.setCommand(command);
        this.request.setArgs(args);
    }

    public void addArgs(List<byte[]> newArgs){
        request.addArgs(newArgs);
    }

    public boolean completeData(ByteBuffer byteBuffer) {
        return RedisProtocol.processResult(byteBuffer,response);
    }

    public boolean fillWriteBuf(ByteBuffer byteBuffer){
        return RedisProtocol.fillCommand(byteBuffer,request);
    }


    public OperationFuture getFuture() {
        return future;
    }

    public void setFuture(OperationFuture future) {
        this.future = future;
    }

    public boolean isFinish() {
        return finish;
    }

    public void setFinish(boolean finish) {
        this.finish = finish;
    }


    public enum Command {
        PING,
        SET,
        GET,
        QUIT,
        EXISTS,
        DEL,
        TYPE,
        FLUSHDB,
        KEYS,
        RANDOMKEY,
        RENAME,
        RENAMENX,
        RENAMEX,
        DBSIZE,
        EXPIRE,
        EXPIREAT,
        TTL,
        SELECT,
        MOVE,
        FLUSHALL,
        GETSET,
        MGET,
        SETNX,
        SETEX,
        MSET,
        MSETNX,
        DECRBY,
        DECR,
        INCRBY,
        INCR,
        APPEND,
        SUBSTR,
        HSET,
        HGET,
        HSETNX,
        HMSET,
        HMGET,
        HINCRBY,
        HEXISTS,
        HDEL,
        HLEN,
        HKEYS,
        HVALS,
        HGETALL,
        RPUSH,
        LPUSH,
        LLEN,
        LRANGE,
        LTRIM,
        LINDEX,
        LSET,
        LREM,
        LPOP,
        RPOP,
        RPOPLPUSH,
        SADD,
        SMEMBERS,
        SREM,
        SPOP,
        SMOVE,
        SCARD,
        SISMEMBER,
        SINTER,
        SINTERSTORE,
        SUNION,
        SUNIONSTORE,
        SDIFF,
        SDIFFSTORE,
        SRANDMEMBER,
        ZADD,
        ZRANGE,
        ZREM,
        ZINCRBY,
        ZRANK,
        ZREVRANK,
        ZREVRANGE,
        ZCARD,
        ZSCORE,
        MULTI,
        DISCARD,
        EXEC,
        WATCH,
        UNWATCH,
        SORT,
        BLPOP,
        BRPOP,
        AUTH,
        SUBSCRIBE,
        PUBLISH,
        UNSUBSCRIBE,
        PSUBSCRIBE,
        PUNSUBSCRIBE,
        ZCOUNT,
        ZRANGEBYSCORE,
        ZREVRANGEBYSCORE,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,
        ZUNIONSTORE,
        ZINTERSTORE,
        SAVE,
        BGSAVE,
        BGREWRITEAOF,
        LASTSAVE,
        SHUTDOWN,
        INFO,
        MONITOR,
        SLAVEOF,
        CONFIG,
        STRLEN,
        SYNC,
        LPUSHX,
        PERSIST,
        RPUSHX,
        ECHO,
        LINSERT,
        DEBUG,
        BRPOPLPUSH,
        SETBIT,
        GETBIT,
        SETRANGE,
        GETRANGE;

        public byte[] getBytes(){
            return StringEncoder.getBytes(this.name());
        }
    }

    public enum Keyword {
        AGGREGATE,
        ALPHA,
        ASC,
        BY,
        DESC,
        GET,
        LIMIT,
        MESSAGE,
        NO,
        NOSORT,
        PMESSAGE,
        PSUBSCRIBE,
        PUNSUBSCRIBE,
        OK,
        ONE,
        QUEUED,
        SET,
        STORE,
        SUBSCRIBE,
        UNSUBSCRIBE,
        WEIGHTS,
        WITHSCORES,
        RESETSTAT,
        RESET,
        FLUSH,
        EXISTS,
        LOAD,
        KILL,
        LEN,
        REFCOUNT,
        ENCODING,
        IDLETIME;

        public byte[] getBytes(){
            return StringEncoder.getBytes(this.name());
        }
    }

    public Request getRequest(){
        return request;
    }

    /**
     * 数据接收完毕，TcpComponent线程把数据set到future里面
     */
    public void pushData(){
        this.future.setResult(response.getData());
        finish=true;
    }




}

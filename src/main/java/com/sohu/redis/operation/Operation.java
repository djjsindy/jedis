package com.sohu.redis.operation;

import com.sohu.redis.protocol.ParseStatus;
import com.sohu.redis.protocol.RedisProtocol;
import com.sohu.redis.protocol.SubParseContext;
import com.sohu.redis.protocol.WritePhase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by jianjundeng on 12/15/13.
 */
public class Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(Operation.class);

    private OperationFuture future;
    /**
     * 解析response的初始状态
     */
    private ParseStatus parseStatus=ParseStatus.RAW;

    /**
     * 保存子操作的状态，负责保持subParseContext和operation相关状态的转换
     */
    private SubParseContext subParseContext=new SubParseContext();

    /**
     * 组装request的初始状态
     */
    private WritePhase writePhase=WritePhase.RAW;

    /**
     * 当前operation需要写的数据，比如key的byte[],value的byte[]等
     */
    private byte[] writeTarget;

    /**
     * 写writeTarget的index，如果当前write的buffer满了需要write到网络，index保存写的索引
     */
    private int writeDataIndex;

    /**
     * 记录多个参数的index
     */
    private int writeArgsIndex;

    /**
     * 读取multi操作response的index
     */
    private int multiDataIndex;

    /**
     * 单个操作中，response数据的长度，get操作等，记录中间数据，
     */
    private StringBuilder dLenStr=new StringBuilder();

    /**
     * 多重操作中，记录结果的个数，记录中间的数据
     */
    private StringBuilder mLenStr=new StringBuilder();

    /**
     * 记录response数据，二维数组是为了记录mget类，多个返回结果的数据
     */
    private byte[][] data;

    /**
     * 记录response是否抛出了异常
     */
    private boolean exception;

    /**
     * data 剩余多少byte未读取
     */
    private int dLast;

    /**
     * response的type
     */
    private ResponseType responseType;

    /**
     * request的command
     */
    private Command command;

    /**
     * 记录request参数
     */
    private byte[][] args;

    /**
     * 接收response数据的bufsize，如果不够了，2倍扩大
     */
    private static int DATABUF_SIZE=1024;

    /**
     *response的data buf的index
     */
    private int dataIndex;

    /**
     * 多重操作中，记录response中，结果个数
     */
    private int mLen;

    public Operation(Command command, byte[]... args) {
        this.command=command;
        this.args=args;
    }

    public boolean completeData(ByteBuffer byteBuffer) {
        return RedisProtocol.processResult(byteBuffer,this);
    }

    public boolean fillWriteBuf(ByteBuffer byteBuffer){
        return RedisProtocol.fillCommand(byteBuffer,this);
    }


    public OperationFuture getFuture() {
        return future;
    }

    public void setFuture(OperationFuture future) {
        this.future = future;
    }

    public ParseStatus getParseStatus() {
        return parseStatus;
    }

    public void setParseStatus(ParseStatus parseStatus) {
        this.parseStatus = parseStatus;
    }

    public StringBuilder getdLenStr() {
        return dLenStr;
    }

    public void setdLenStr(StringBuilder dLenStr) {
        this.dLenStr = dLenStr;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public void setResponseType(ResponseType responseType) {
        this.responseType = responseType;
    }

    public WritePhase getWritePhase() {
        return writePhase;
    }

    public void setWritePhase(WritePhase writePhase) {
        this.writePhase = writePhase;
    }

    public byte[] getWriteTarget() {
        return writeTarget;
    }

    public void setWriteTarget(byte[] writeTarget) {
        this.writeTarget = writeTarget;
    }


    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public byte[][] getArgs() {
        return args;
    }

    public void setArgs(byte[][] args) {
        this.args = args;
    }

    public int getWriteDataIndex() {
        return writeDataIndex;
    }

    public void setWriteDataIndex(int writeDataIndex) {
        this.writeDataIndex = writeDataIndex;
    }

    public int getWriteArgsIndex() {
        return writeArgsIndex;
    }

    public void setWriteArgsIndex(int writeArgsIndex) {
        this.writeArgsIndex = writeArgsIndex;
    }

    public boolean isException() {
        return exception;
    }

    public void setException(boolean exception) {
        this.exception = exception;
    }

    public int getMutilDataIndex() {
        return multiDataIndex;
    }

    public void setMutilDataIndex(int multiDataIndex) {
        this.multiDataIndex = multiDataIndex;
    }

    public StringBuilder getmLenStr() {
        return mLenStr;
    }

    public void setmLenStr(StringBuilder mLenstr) {
        this.mLenStr = mLenstr;
    }

    public int getmLen() {
        return mLen;
    }

    public void setmLen(int mLen) {
        this.mLen = mLen;
    }

    public int getdLast() {
        return dLast;
    }

    public void setdLast(int dLast) {
        this.dLast = dLast;
    }

    public SubParseContext getSubParseContext() {
        return subParseContext;
    }

    public void setSubParseContext(SubParseContext subParseContext) {
        this.subParseContext = subParseContext;
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
    }

    /**
     * 普通bulk multi bulk操作返回数据的回调方法，
     * @param byteBuffer
     * @param length 写入数据的长度
     */
    public void addData(ByteBuffer byteBuffer,int length){
        byte[] temp;
        int offset=0;
        if(data==null){
            data=new byte[mLen==0?1:mLen][];
        }
        if(data[multiDataIndex]==null){
            temp=new byte[length];
        }else{
            offset=data[multiDataIndex].length-1;
            temp=new byte[data[multiDataIndex].length+length];
            System.arraycopy(data[multiDataIndex],0,temp,0,data[multiDataIndex].length);
        }
        if(byteBuffer!=null)
            byteBuffer.get(temp,offset,length);
        data[multiDataIndex]=temp;
    }

    /**
     * 返回status code，异常信息的回调方法，一个byte一个byte的写
     * @param b
     */
    public void addMsgData(byte b){
        if(data==null){
            data=new byte[mLen==0?1:mLen][];
        }
        if(data[multiDataIndex]==null){
            data[multiDataIndex]=new byte[DATABUF_SIZE];
        }
        if(data[multiDataIndex].length-1==dataIndex){
            DATABUF_SIZE*=2;
            byte[] temp=new byte[DATABUF_SIZE];
            System.arraycopy(data[multiDataIndex],0,temp,0,data[multiDataIndex].length);
            data[multiDataIndex]=temp;
        }
        data[multiDataIndex][dataIndex]=b;
        dataIndex++;
    }

    /**
     * 接收完全部数据，去掉data中最后的空数据
     */
    public void compactMsgData(){
        byte[] temp=new byte[dataIndex];
        System.arraycopy(data[multiDataIndex],0,temp,0,dataIndex);
        data[multiDataIndex]=temp;
    }

    /**
     * 数据接收完毕，TcpComponent线程把数据set到future里面
     */
    public void pushData(){
        this.future.setResult(data);
    }


}

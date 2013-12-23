package com.sohu.redis.protocol;

import com.sohu.redis.operation.*;
import com.sohu.redis.transform.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by jianjundeng on 12/17/13.
 */
public class RedisProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisProtocol.class);

    public static final byte DOLLAR_BYTE = '$';

    public static final byte ASTERISK_BYTE = '*';

    public static final byte PLUS_BYTE = '+';

    public static final byte MINUS_BYTE = '-';

    public static final byte COLON_BYTE = ':';

    public static final byte specialr = '\r';

    public static final byte specialn = '\n';

    /**
     * 把int组装成byte array
     * @param value
     * @return
     */
    private static byte[] buildInt(int value) {
        byte[] temp=new byte[20];
        String str=Integer.toString(value);
        int index=0;
        for(char c:str.toCharArray()){
            temp[index++]=(byte)c;
        }
        byte[] result=new byte[index];
        System.arraycopy(temp,0,result,0,index);
        return result;
    }

    public static boolean processResult(ByteBuffer buffer,Operation operation) {
        if(operation.getParseStatus()==ParseStatus.RAW){
            byte b = buffer.get();
            switch (b){
                case MINUS_BYTE:
                    operation.setResponseType(ResponseType.SimpleReply);
                    operation.setException(true);
                    return processSimpleReply(buffer, operation);
                case ASTERISK_BYTE:
                    operation.setResponseType(ResponseType.MutilReply);
                    return processMultiBulkReply(buffer, operation);
                case COLON_BYTE:
                case PLUS_BYTE:
                    operation.setResponseType(ResponseType.SimpleReply);
                    return processSimpleReply(buffer, operation);
                case DOLLAR_BYTE:
                    operation.setResponseType(ResponseType.BulkReply);
                    return processBulkReply(buffer,operation);
                default:
                    return true;
            }              
        }else{
            switch (operation.getResponseType()){
                case SimpleReply:
                    return processSimpleReply(buffer,operation);
                case BulkReply:
                    return processBulkReply(buffer, operation);
                case MutilReply:
                    return processMultiBulkReply(buffer,operation);
                default:
                    return true;
            }
        }

    }

    private static boolean  processMultiBulkReply(ByteBuffer byteBuffer,Operation operation) {
        while(byteBuffer.position()<byteBuffer.limit()){
            if(operation.getParseStatus()==ParseStatus.RAW){
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH);
            }
            else if(operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH_N){
                //取出上次的解析状态
                operation.setParseStatus(operation.getSubParseContext().getParseStatus());
                operation.setResponseType(operation.getSubParseContext().getResponseType());
                boolean complete=processResult(byteBuffer,operation);
                //还原主请求状态
                operation.getSubParseContext().setParseStatus(operation.getParseStatus());
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
                operation.getSubParseContext().setResponseType(operation.getResponseType());
                operation.setResponseType(ResponseType.MutilReply);
                if(complete){
                    int mutilDataIndex=operation.getMutilDataIndex();
                    if(mutilDataIndex==operation.getmLen()-1){
                        return true;
                    }else{
                        operation.setMutilDataIndex(mutilDataIndex+1);
                        operation.setdLenStr(new StringBuilder());
                        operation.getSubParseContext().setParseStatus(ParseStatus.RAW);
                    }
                }else{
                    return false;
                }
                continue;
            }
            int b=byteBuffer.get();
            if(b=='\r'&&operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH){
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_R);
            }else if(b=='\n'&&operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH_R){
                int length=Integer.parseInt(operation.getmLenstr().toString());
                operation.setmLen(length);
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
            }else if(operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH){
                operation.getmLenstr().append((char)b);
            }


        }
        return false;
    }

    private static boolean processSimpleReply(ByteBuffer byteBuffer,Operation operation) {
        while(byteBuffer.position()<byteBuffer.limit()){
            if(operation.getParseStatus()==ParseStatus.RAW){
                operation.setParseStatus(ParseStatus.READ_MSG);
            }
            int b=byteBuffer.get();
            if(b=='\r'&&operation.getParseStatus()==ParseStatus.READ_MSG){
                operation.setParseStatus(ParseStatus.MSG_R);
            }else if(b=='\n'&& operation.getParseStatus()==ParseStatus.MSG_R){
                operation.compactMsgData();
                return true;
            }
            else if(operation.getParseStatus()==ParseStatus.READ_MSG){
                operation.addMsgData((byte)b);
            }
        }
        return false;
    }

    /**
     * 解析请求
     * @param byteBuffer
     * @param operation
     * @return
     */
    private static boolean processBulkReply(ByteBuffer byteBuffer,Operation operation) {
        while(byteBuffer.position()<byteBuffer.limit()){
            if(operation.getParseStatus()==ParseStatus.MEETLENGTHN){
                int len=operation.getdLast();
                int remaining=byteBuffer.remaining();
                if(len<=remaining){
                    operation.addData(byteBuffer,len);
                    operation.setParseStatus(ParseStatus.DATAEND);
                }else{
                    operation.addData(byteBuffer, remaining);
                    operation.setdLast(len-remaining);
                    return false;
                }
                continue;
            }
            int b=byteBuffer.get();
            if(b=='\r'&&operation.getParseStatus()==ParseStatus.READLENGTH){
                operation.setParseStatus(ParseStatus.MEETLENGTHR);
            }
            if(b=='\n'&&operation.getParseStatus()==ParseStatus.MEETLENGTHR){
                operation.setParseStatus(ParseStatus.MEETLENGTHN);
                int length=Integer.parseInt(operation.getdLenStr().toString());
                if(length>=0){
                    operation.setdLast(length);
                }else{
                    operation.addData(null,0);
                    return true;
                }
            }
            if(operation.getParseStatus()==ParseStatus.READLENGTH){
                operation.getdLenStr().append((char)b);
            }
            if(operation.getParseStatus()==ParseStatus.RAW){
                operation.setParseStatus(ParseStatus.READLENGTH);
                operation.getdLenStr().append((char)b);
            }
            if(b=='\r'&&operation.getParseStatus()==ParseStatus.DATAEND){
                operation.setParseStatus(ParseStatus.MEETDATAR);
            }
            if(b=='\n'&&operation.getParseStatus()==ParseStatus.MEETDATAR){
                return true;
            }
        }
        return false;
    }

    /**
     * 组装writebyte
     * @param byteBuffer
     * @param operation
     * @return
     */
    public static boolean fillCommand(ByteBuffer byteBuffer,Operation operation){
        while(byteBuffer.position()<byteBuffer.limit()){
            switch (operation.getWritePhase()){
                case RAW:
                    byteBuffer.put(ASTERISK_BYTE);
                    operation.setWriteArgsIndex(0);
                    prepareWriteData(buildInt(operation.getArgs().length + 1),WritePhase.WRITE_ARGS_LENGTH,operation);
                    break;
                case WRITE_ARGS_LENGTH:
                    writeData(operation, byteBuffer, WritePhase.WRITE_ARGS_LENGTH_END);
                    break;
                case WRITE_ARGS_LENGTH_END:
                    byteBuffer.put(specialr);
                    operation.setWritePhase(WritePhase.WRITE_ARGS_R);
                    break;
                case WRITE_ARGS_R:
                    byteBuffer.put(specialn);
                    operation.setWritePhase(WritePhase.WRITE_ARGS_N);
                    break;
                case WRITE_ARGS_N:
                    byteBuffer.put(DOLLAR_BYTE);
                    operation.setWritePhase(WritePhase.WRITE_DOLLAR);
                    break;
                case WRITE_DOLLAR:
                    prepareWriteData(buildInt(StringEncoder.getBytes(operation.getCommand().name()).length),WritePhase.WRITE_COMMAND_LENGTH,operation);
                    break;
                case WRITE_COMMAND_LENGTH:
                    writeData(operation,byteBuffer,WritePhase.WRITE_COMMAND_LENGTH_R);
                    break;
                case WRITE_COMMAND_LENGTH_R:
                    byteBuffer.put(specialr);
                    operation.setWritePhase(WritePhase.WRITE_COMMAND_LENGTH_N);
                    break;
                case WRITE_COMMAND_LENGTH_N:
                    byteBuffer.put(specialn);
                    prepareWriteData(StringEncoder.getBytes(operation.getCommand().toString()),WritePhase.WRITE_COMMAND,operation);
                    break;
                case WRITE_COMMAND:
                    writeData(operation,byteBuffer,WritePhase.WRITE_COMMAND_R);
                    break;
                case WRITE_COMMAND_R:
                    byteBuffer.put(specialr);
                    operation.setWritePhase(WritePhase.WRITE_COMMAND_N);
                    break;
                case WRITE_COMMAND_N:
                    byteBuffer.put(specialn);
                    operation.setWritePhase(WritePhase.WRITE_ARGS_DOLLAR);
                    break;
                case WRITE_ARGS_DOLLAR:
                    if(operation.getArgs().length==0||operation.getWriteArgsIndex()==operation.getArgs().length){
                        return true;
                    }
                    byteBuffer.put(DOLLAR_BYTE);
                    prepareWriteData(buildInt((operation.getArgs()[operation.getWriteArgsIndex()]).length),WritePhase.WRITE_ARG_LENGTH,operation);
                    break;
                case WRITE_ARG_LENGTH:
                    writeData(operation, byteBuffer, WritePhase.WRITE_ARG_LENGTH_R);
                    break;
                case WRITE_ARG_LENGTH_R:
                    byteBuffer.put(specialr);
                    operation.setWritePhase(WritePhase.WRITE_ARG_LENGTH_N);
                    break;
                case WRITE_ARG_LENGTH_N:
                    byteBuffer.put(specialn);
                    prepareWriteData(operation.getArgs()[operation.getWriteArgsIndex()],WritePhase.WRITE_ARG,operation);
                    break;
                case WRITE_ARG:
                    writeData(operation,byteBuffer,WritePhase.WRITE_ARG_R);
                    break;
                case WRITE_ARG_R:
                    byteBuffer.put(specialr);
                    operation.setWritePhase(WritePhase.WRITE_ARG_N);
                    break;
                case WRITE_ARG_N:
                    byteBuffer.put(specialn);
                    operation.setWriteArgsIndex(operation.getWriteArgsIndex()+1);
                    operation.setWritePhase(WritePhase.WRITE_ARGS_DOLLAR);
                    break;
            }
        }
        return false;
    }

    /**
     * 设置数据，index
     * @param data
     * @param toPhase
     * @param operation
     */
    private static void prepareWriteData(byte[] data,WritePhase toPhase,Operation operation){
        operation.setWriteTarget(data);
        operation.setWritePhase(toPhase);
        operation.setWriteDataIndex(0);
    }

    /**
     * 写数据per bit到byte buffer
     * @param operation
     * @param byteBuffer
     * @param toPhase
     */
    private static void writeData(Operation operation,ByteBuffer byteBuffer,WritePhase toPhase){
        int writeIndex=operation.getWriteDataIndex();
        if(writeIndex<=operation.getWriteTarget().length-1){
            byteBuffer.put(operation.getWriteTarget()[writeIndex]);
        }
        if(writeIndex==operation.getWriteTarget().length-1){
            operation.setWritePhase(toPhase);
        }else{
            operation.setWriteDataIndex(writeIndex+1);
        }
    }
}

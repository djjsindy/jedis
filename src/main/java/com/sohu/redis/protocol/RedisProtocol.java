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

    public static final byte DOLLAR_BYTE = '$';

    public static final byte ASTERISK_BYTE = '*';

    public static final byte PLUS_BYTE = '+';

    public static final byte MINUS_BYTE = '-';

    public static final byte COLON_BYTE = ':';

    public static final byte specialR = '\r';

    public static final byte specialN = '\n';

    /**
     * 把int组装成byte array
     * @param value
     * @return
     */
    private static byte[] buildInt(int value) {
        return Integer.toString(value).getBytes();
    }

    public static boolean processResult(ByteBuffer buffer,Operation operation) {
        if(operation.getParseStatus()==ParseStatus.RAW){
            byte b = buffer.get();
            switch (b){
                case MINUS_BYTE:
                    operation.setResponseType(ResponseType.SimpleReply);
                    operation.setException(true);
                    operation.setParseStatus(ParseStatus.READ_MSG);
                    return processSimpleReply(buffer, operation);
                case ASTERISK_BYTE:
                    operation.setResponseType(ResponseType.MultiReply);
                    operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH);
                    return processMultiBulkReply(buffer, operation);
                case COLON_BYTE:
                case PLUS_BYTE:
                    operation.setResponseType(ResponseType.SimpleReply);
                    operation.setParseStatus(ParseStatus.READ_MSG);
                    return processSimpleReply(buffer, operation);
                case DOLLAR_BYTE:
                    operation.setResponseType(ResponseType.BulkReply);
                    operation.setParseStatus(ParseStatus.READ_LENGTH);
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
                case MultiReply:
                    return processMultiBulkReply(buffer,operation);
                default:
                    return true;
            }
        }

    }

    private static boolean  processMultiBulkReply(ByteBuffer byteBuffer,Operation operation) {
        while(byteBuffer.position()<byteBuffer.limit()){
             if(operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH_N){
                //取出上次的解析状态
                loadSubContext(operation);
                boolean complete=processResult(byteBuffer,operation);
                //还原主请求状态
                loadPrimaryContext(operation);
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
                operation.setResponseType(ResponseType.MultiReply);
                if(complete){
                    int multiDataIndex=operation.getMutilDataIndex();
                    if(multiDataIndex==operation.getmLen()-1){
                        //解析到最后一个multi response了
                        return true;
                    }else{
                        //设置index+1
                        operation.setMutilDataIndex(multiDataIndex+1);
                        //由于dLenStr每次都是append操作，这里需要清空
                        operation.setdLenStr(new StringBuilder());
                        //还原subContext的状态，为解析下一个sub response做准备
                        operation.getSubParseContext().clear();
                    }
                }else{
                    return false;
                }
                continue;
            }
            int b=byteBuffer.get();
            if(b==specialR&&operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH){
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_R);
            }else if(b==specialN&&operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH_R){
                int length=Integer.parseInt(operation.getmLenStr().toString());
                operation.setmLen(length);
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
            }else if(operation.getParseStatus()==ParseStatus.READ_RESULT_LENGTH){
                operation.getmLenStr().append((char)b);
            }
        }
        return false;
    }

    /**
     * 从sub context中load主operation的状态
     * @param operation
     */
    private static void loadPrimaryContext(Operation operation) {
        operation.getSubParseContext().setParseStatus(operation.getParseStatus());
        operation.getSubParseContext().setResponseType(operation.getResponseType());
    }

    /**
     * 加载sub context到operation中，表示开始解析子response
     * @param operation
     */
    private static void loadSubContext(Operation operation) {
        operation.setParseStatus(operation.getSubParseContext().getParseStatus());
        operation.setResponseType(operation.getSubParseContext().getResponseType());
    }

    private static boolean processSimpleReply(ByteBuffer byteBuffer,Operation operation) {
        while(byteBuffer.position()<byteBuffer.limit()){
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
            //按照byte buffer的remaining和实际这块数据大小做对比
            //如果实际这块数据的数据量大于remaining，说明read数据没有read完全，需要继续read
            //否则就说明remaining中的数据，完全满足真是数据量的大小，那么直接批量copy就行，设置next状态
            if(operation.getParseStatus()==ParseStatus.READ_LENGTH_N){
                int len=operation.getdLast();
                int remaining=byteBuffer.remaining();
                if(len<=remaining){
                    operation.addData(byteBuffer,len);
                    operation.setParseStatus(ParseStatus.READ_DATA_END);
                }else{
                    operation.addData(byteBuffer, remaining);
                    operation.setdLast(len-remaining);
                    return false;
                }
                continue;
            }
            int b=byteBuffer.get();
            switch (operation.getParseStatus()){
                case READ_LENGTH:
                    if(b=='\r'){
                        operation.setParseStatus(ParseStatus.READ_LENGTH_R);
                    }else{
                        operation.getdLenStr().append((char)b);
                    }
                    break;
                case READ_LENGTH_R:
                    if(b=='\n'){
                        operation.setParseStatus(ParseStatus.READ_LENGTH_N);
                        int length=Integer.parseInt(operation.getdLenStr().toString());
                        if(length>=0){
                            operation.setdLast(length);
                        }else{
                            operation.addData(null,0);
                            return true;
                        }
                    }
                    break;
                case READ_DATA_END:
                    if(b=='\r'){
                        operation.setParseStatus(ParseStatus.READ_DATA_R);
                    }
                    break;
                case READ_DATA_R:
                    if(b=='\n')
                        return true;
            }
        }
        return false;
    }

    /**
     * 组装write byteBuffer，规定多种状态的目的在于，在byte buffer满的情况下（写入socketchannel），保存每一步写入进度。
     * RAW:初始状态，byteBuffer开始写入协议最开始的星号，设置写入参数的index，向operation中填充需要写的参数长度的数据，下一阶段开始写入设置好的数据，直接转入WRITE_ARGS_LENGTH状态
     * WRITE_ARGS_LENGTH：开始写入参数长度数据，每次写入1 byte，根据数据的operation中的writeIndex来决定是否写入完毕，如果写入完毕转到WRITE_ARGS_R
     * WRITE_ARGS_R:写入 '\r',写完转入WRITE_ARGS_N
     * WRITE_ARGS_N:写入 '\n',写完转入WRITE_DOLLAR
     * WRITE_DOLLAR:写入 '$',同时准备下一步写入的数据command的名称长度，设置operation的writeTarget和writeIndex，转入WRITE_COMMAND_LENGTH
     * WRITE_COMMAND_LENGTH:byte buffer写入command名称，每byte写入byte buffer，写入完成转入WRITE_COMMAND_LENGTH_R
     * WRITE_COMMAND_LENGTH_R:写入'\r',完成后转入WRITE_COMMAND_LENGTH_N
     * WRITE_COMMAND_LENGTH_N:写入'\n',准备写入command的名称的数据，完成转入WRITE_COMMAND
     * WRITE_COMMAND:写入byte buffer command名称的byte 数据，每次一个byte，写完成了转入WRITE_COMMAND_R
     * WRITE_COMMAND_R:写入'\r',写完转入WRITE_COMMAND_N
     * WRITE_COMMAND_N:写入'\n',写完转入WRITE_ARGS
     * WRITE_ARGS:这个状态来根据是否有参数来决定是否退出fill command，如果没有参数，那么byte buffer就填充完毕，如果有参数那么跳转到WRITE_ARGS_DOLLAR
     * WRITE_ARGS_DOLLAR:写入'$',准备参数byte数量的数据,转入WRITE_ARG_LENGTH
     * WRITE_ARG_LENGTH:开始写入当前参数byte数量的数据，写完转入WRITE_ARG_LENGTH_R
     * WRITE_ARG_LENGTH_R:写入'\r',转入WRITE_ARG_LENGTH_N
     * WRITE_ARG_LENGTH_N:写入'\n',准备当前参数byte数据，转入WRITE_ARG
     * WRITE_ARG:写入当前参数byte数据，一次写入1byte，写完转入WRITE_ARG_R
     * WRITE_ARG_R:写入'\r',转入WRITE_ARG_N
     * WRITE_ARG_N:写入'\n',转入WRITE_ARGS
     * @param byteBuffer
     * @param operation
     * @return
     */
    public static boolean fillCommand(ByteBuffer byteBuffer,Operation operation){
        while(byteBuffer.position()<byteBuffer.limit()){
            switch (operation.getWritePhase()){
                case RAW:
                    byteBuffer.put(ASTERISK_BYTE);
                    prepareWriteData(buildInt(operation.getArgs().length + 1),WritePhase.WRITE_ARGS_LENGTH,operation);
                    break;
                case WRITE_ARGS_LENGTH:
                    writeData(operation, byteBuffer, WritePhase.WRITE_ARGS_R);
                    break;
                case WRITE_ARGS_R:
                    byteBuffer.put(specialR);
                    operation.setWritePhase(WritePhase.WRITE_ARGS_N);
                    break;
                case WRITE_ARGS_N:
                    byteBuffer.put(specialN);
                    operation.setWritePhase(WritePhase.WRITE_DOLLAR);
                    break;
                case WRITE_DOLLAR:
                    byteBuffer.put(DOLLAR_BYTE);
                    prepareWriteData(buildInt(StringEncoder.getBytes(operation.getCommand().name()).length),WritePhase.WRITE_COMMAND_LENGTH,operation);
                    break;
                case WRITE_COMMAND_LENGTH:
                    writeData(operation,byteBuffer,WritePhase.WRITE_COMMAND_LENGTH_R);
                    break;
                case WRITE_COMMAND_LENGTH_R:
                    byteBuffer.put(specialR);
                    operation.setWritePhase(WritePhase.WRITE_COMMAND_LENGTH_N);
                    break;
                case WRITE_COMMAND_LENGTH_N:
                    byteBuffer.put(specialN);
                    prepareWriteData(StringEncoder.getBytes(operation.getCommand().toString()),WritePhase.WRITE_COMMAND,operation);
                    break;
                case WRITE_COMMAND:
                    writeData(operation,byteBuffer,WritePhase.WRITE_COMMAND_R);
                    break;
                case WRITE_COMMAND_R:
                    byteBuffer.put(specialR);
                    operation.setWritePhase(WritePhase.WRITE_COMMAND_N);
                    break;
                case WRITE_COMMAND_N:
                    byteBuffer.put(specialN);
                    operation.setWritePhase(WritePhase.WRITE_ARGS);
                    //这里没有break，抽象出WRITE_ARGS，为了分离代码
                case WRITE_ARGS:
                    if(operation.getArgs().length==0||operation.getWriteArgsIndex()==operation.getArgs().length){
                        return true;
                    }
                    operation.setWritePhase(WritePhase.WRITE_ARGS_DOLLAR);
                    break;
                case WRITE_ARGS_DOLLAR:
                    byteBuffer.put(DOLLAR_BYTE);
                    prepareWriteData(buildInt((operation.getArgs()[operation.getWriteArgsIndex()]).length),WritePhase.WRITE_ARG_LENGTH,operation);
                    break;
                case WRITE_ARG_LENGTH:
                    writeData(operation, byteBuffer, WritePhase.WRITE_ARG_LENGTH_R);
                    break;
                case WRITE_ARG_LENGTH_R:
                    byteBuffer.put(specialR);
                    operation.setWritePhase(WritePhase.WRITE_ARG_LENGTH_N);
                    break;
                case WRITE_ARG_LENGTH_N:
                    byteBuffer.put(specialN);
                    prepareWriteData(operation.getArgs()[operation.getWriteArgsIndex()],WritePhase.WRITE_ARG,operation);
                    break;
                case WRITE_ARG:
                    writeData(operation,byteBuffer,WritePhase.WRITE_ARG_R);
                    break;
                case WRITE_ARG_R:
                    byteBuffer.put(specialR);
                    operation.setWritePhase(WritePhase.WRITE_ARG_N);
                    break;
                case WRITE_ARG_N:
                    byteBuffer.put(specialN);
                    operation.setWriteArgsIndex(operation.getWriteArgsIndex()+1);//设置写入下一个参数，如果有继续写，没有的话由WRITE_ARGS返回true
                    operation.setWritePhase(WritePhase.WRITE_ARGS);
                    break;
            }
        }
        return false;
    }

    /**
     * 设置要写的数据，index，设置operation的next状态
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
     * 写数据per byte buffer
     * @param operation
     * @param byteBuffer
     * @param toPhase
     */
    private static void writeData(Operation operation,ByteBuffer byteBuffer,WritePhase toPhase){
        int writeIndex=operation.getWriteDataIndex();
        if(writeIndex<=operation.getWriteTarget().length-1){
            byteBuffer.put(operation.getWriteTarget()[writeIndex]);
            operation.setWriteDataIndex(writeIndex + 1);
        }else if(writeIndex==operation.getWriteTarget().length)
            operation.setWritePhase(toPhase);
    }
}

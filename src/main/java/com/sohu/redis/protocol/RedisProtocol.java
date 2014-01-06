package com.sohu.redis.protocol;

import com.sohu.redis.model.PubSubCallBack;
import com.sohu.redis.operation.*;
import com.sohu.redis.transform.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

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
     *
     * @param value
     * @return
     */
    private static byte[] buildInt(int value) {
        return Integer.toString(value).getBytes();
    }

    private static Long buildLong(byte[] response) {
        StringBuilder sb = new StringBuilder();
        for (byte b : response) {
            sb.append((char) b);
        }
        return Long.valueOf(sb.toString());
    }

    public static boolean processResult(ByteBuffer buffer, Response response) {
        if (response.getParseStatus() == ParseStatus.RAW) {
            byte b = buffer.get();
            switch (b) {
                case MINUS_BYTE:
                    response.setResponseType(ResponseType.SimpleReply);
                    response.setException(true);
                    response.setParseStatus(ParseStatus.READ_MSG);
                    return processSimpleReply(buffer, response);
                case ASTERISK_BYTE:
                    response.setResponseType(ResponseType.MultiReply);
                    response.setParseStatus(ParseStatus.READ_RESULT_LENGTH);
                    return processMultiBulkReply(buffer, response);
                case COLON_BYTE:
                case PLUS_BYTE:
                    response.setResponseType(ResponseType.SimpleReply);
                    response.setParseStatus(ParseStatus.READ_MSG);
                    return processSimpleReply(buffer, response);
                case DOLLAR_BYTE:
                    response.setResponseType(ResponseType.BulkReply);
                    response.setParseStatus(ParseStatus.READ_LENGTH);
                    return processBulkReply(buffer, response);
                default:
                    return true;
            }
        } else {
            switch (response.getResponseType()) {
                case SimpleReply:
                    return processSimpleReply(buffer, response);
                case BulkReply:
                    return processBulkReply(buffer, response);
                case MultiReply:
                    return processMultiBulkReply(buffer, response);
                default:
                    return true;
            }
        }

    }

    private static boolean processMultiBulkReply(ByteBuffer byteBuffer, Response response) {
        while (byteBuffer.position() < byteBuffer.limit()) {
            if (response.getParseStatus() == ParseStatus.READ_RESULT_LENGTH_N) {
                //取出上次的解析状态
                loadSubContext(response);
                boolean complete = processResult(byteBuffer, response);
                //还原主请求状态
                loadPrimaryContext(response);
                response.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
                response.setResponseType(ResponseType.MultiReply);
                if (complete) {
                    int multiDataIndex = response.getMultiDataIndex();
                    if (multiDataIndex == response.getmLen() - 1) {
                        //解析到最后一个multi response了
                        return true;
                    } else {
                        //设置index+1
                        response.setMultiDataIndex(multiDataIndex + 1);
                        //由于dLenStr每次都是append操作，这里需要清空
                        response.setdLenStr(new StringBuilder());
                        //还原subContext的状态，为解析下一个sub response做准备
                        response.getSubParseContext().clear();
                    }
                } else {
                    return false;
                }
                continue;
            }
            int b = byteBuffer.get();
            if (b == specialR && response.getParseStatus() == ParseStatus.READ_RESULT_LENGTH) {
                response.setParseStatus(ParseStatus.READ_RESULT_LENGTH_R);
            } else if (b == specialN && response.getParseStatus() == ParseStatus.READ_RESULT_LENGTH_R) {
                int length = Integer.parseInt(response.getmLenStr().toString());
                response.setmLen(length);
                response.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
            } else if (response.getParseStatus() == ParseStatus.READ_RESULT_LENGTH) {
                response.getmLenStr().append((char) b);
            }
        }
        return false;
    }

    /**
     * 从sub context中load主operation的状态
     *
     * @param response
     */
    private static void loadPrimaryContext(Response response) {
        response.getSubParseContext().setParseStatus(response.getParseStatus());
        response.getSubParseContext().setResponseType(response.getResponseType());
    }

    /**
     * 加载sub context到operation中，表示开始解析子response
     *
     * @param response
     */
    private static void loadSubContext(Response response) {
        response.setParseStatus(response.getSubParseContext().getParseStatus());
        response.setResponseType(response.getSubParseContext().getResponseType());
    }

    private static boolean processSimpleReply(ByteBuffer byteBuffer, Response response) {
        while (byteBuffer.position() < byteBuffer.limit()) {
            int b = byteBuffer.get();
            if (b == '\r' && response.getParseStatus() == ParseStatus.READ_MSG) {
                response.setParseStatus(ParseStatus.MSG_R);
            } else if (b == '\n' && response.getParseStatus() == ParseStatus.MSG_R) {
                response.compactMsgData();
                return true;
            } else if (response.getParseStatus() == ParseStatus.READ_MSG) {
                response.addMsgData((byte) b);
            }
        }
        return false;
    }

    /**
     * 解析请求
     *
     * @param byteBuffer
     * @param response
     * @return
     */
    private static boolean processBulkReply(ByteBuffer byteBuffer, Response response) {
        while (byteBuffer.position() < byteBuffer.limit()) {
            //按照byte buffer的remaining和实际这块数据大小做对比
            //如果实际这块数据的数据量大于remaining，说明read数据没有read完全，需要继续read
            //否则就说明remaining中的数据，完全满足真是数据量的大小，那么直接批量copy就行，设置next状态
            if (response.getParseStatus() == ParseStatus.READ_LENGTH_N) {
                int len = response.getdLast();
                int remaining = byteBuffer.remaining();
                if (len <= remaining) {
                    response.addData(byteBuffer, len);
                    response.setParseStatus(ParseStatus.READ_DATA_END);
                } else {
                    response.addData(byteBuffer, remaining);
                    response.setdLast(len - remaining);
                    return false;
                }
                continue;
            }
            int b = byteBuffer.get();
            switch (response.getParseStatus()) {
                case READ_LENGTH:
                    if (b == '\r') {
                        response.setParseStatus(ParseStatus.READ_LENGTH_R);
                    } else {
                        response.getdLenStr().append((char) b);
                    }
                    break;
                case READ_LENGTH_R:
                    if (b == '\n') {
                        response.setParseStatus(ParseStatus.READ_LENGTH_N);
                        int length = Integer.parseInt(response.getdLenStr().toString());
                        if (length >= 0) {
                            response.setdLast(length);
                        } else {
                            response.addData(null, 0);
                            return true;
                        }
                    }
                    break;
                case READ_DATA_END:
                    if (b == '\r') {
                        response.setParseStatus(ParseStatus.READ_DATA_R);
                    }
                    break;
                case READ_DATA_R:
                    if (b == '\n')
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
     *
     * @param byteBuffer
     * @param request
     * @return
     */
    public static boolean fillCommand(ByteBuffer byteBuffer, Request request) {
        while (byteBuffer.position() < byteBuffer.limit()) {
            switch (request.getWritePhase()) {
                case RAW:
                    byteBuffer.put(ASTERISK_BYTE);
                    prepareWriteData(buildInt(request.getArgs().length + 1), WritePhase.WRITE_ARGS_LENGTH, request);
                    break;
                case WRITE_ARGS_LENGTH:
                    writeData(request, byteBuffer, WritePhase.WRITE_ARGS_R);
                    break;
                case WRITE_ARGS_R:
                    byteBuffer.put(specialR);
                    request.setWritePhase(WritePhase.WRITE_ARGS_N);
                    break;
                case WRITE_ARGS_N:
                    byteBuffer.put(specialN);
                    request.setWritePhase(WritePhase.WRITE_DOLLAR);
                    break;
                case WRITE_DOLLAR:
                    byteBuffer.put(DOLLAR_BYTE);
                    prepareWriteData(buildInt(StringEncoder.getBytes(request.getCommand().name()).length), WritePhase.WRITE_COMMAND_LENGTH, request);
                    break;
                case WRITE_COMMAND_LENGTH:
                    writeData(request, byteBuffer, WritePhase.WRITE_COMMAND_LENGTH_R);
                    break;
                case WRITE_COMMAND_LENGTH_R:
                    byteBuffer.put(specialR);
                    request.setWritePhase(WritePhase.WRITE_COMMAND_LENGTH_N);
                    break;
                case WRITE_COMMAND_LENGTH_N:
                    byteBuffer.put(specialN);
                    prepareWriteData(StringEncoder.getBytes(request.getCommand().toString()), WritePhase.WRITE_COMMAND, request);
                    break;
                case WRITE_COMMAND:
                    writeData(request, byteBuffer, WritePhase.WRITE_COMMAND_R);
                    break;
                case WRITE_COMMAND_R:
                    byteBuffer.put(specialR);
                    request.setWritePhase(WritePhase.WRITE_COMMAND_N);
                    break;
                case WRITE_COMMAND_N:
                    byteBuffer.put(specialN);
                    request.setWritePhase(WritePhase.WRITE_ARGS);
                    //这里没有break，抽象出WRITE_ARGS，为了分离代码
                case WRITE_ARGS:
                    if (request.getArgs().length == 0 || request.getWriteArgsIndex() == request.getArgs().length) {
                        return true;
                    }
                    request.setWritePhase(WritePhase.WRITE_ARGS_DOLLAR);
                    break;
                case WRITE_ARGS_DOLLAR:
                    byteBuffer.put(DOLLAR_BYTE);
                    prepareWriteData(buildInt((request.getArgs()[request.getWriteArgsIndex()]).length), WritePhase.WRITE_ARG_LENGTH, request);
                    break;
                case WRITE_ARG_LENGTH:
                    writeData(request, byteBuffer, WritePhase.WRITE_ARG_LENGTH_R);
                    break;
                case WRITE_ARG_LENGTH_R:
                    byteBuffer.put(specialR);
                    request.setWritePhase(WritePhase.WRITE_ARG_LENGTH_N);
                    break;
                case WRITE_ARG_LENGTH_N:
                    byteBuffer.put(specialN);
                    prepareWriteData(request.getArgs()[request.getWriteArgsIndex()], WritePhase.WRITE_ARG, request);
                    break;
                case WRITE_ARG:
                    writeData(request, byteBuffer, WritePhase.WRITE_ARG_R);
                    break;
                case WRITE_ARG_R:
                    byteBuffer.put(specialR);
                    request.setWritePhase(WritePhase.WRITE_ARG_N);
                    break;
                case WRITE_ARG_N:
                    byteBuffer.put(specialN);
                    request.setWriteArgsIndex(request.getWriteArgsIndex() + 1);//设置写入下一个参数，如果有继续写，没有的话由WRITE_ARGS返回true
                    request.setWritePhase(WritePhase.WRITE_ARGS);
                    break;
            }
        }
        return false;
    }

    /**
     * 设置要写的数据，index，设置operation的next状态
     *
     * @param data
     * @param toPhase
     * @param request
     */
    private static void prepareWriteData(byte[] data, WritePhase toPhase, Request request) {
        request.setWriteTarget(data);
        request.setWritePhase(toPhase);
        request.setWriteDataIndex(0);
    }

    /**
     * 写数据per byte buffer
     *
     * @param request
     * @param byteBuffer
     * @param toPhase
     */
    private static void writeData(Request request, ByteBuffer byteBuffer, WritePhase toPhase) {
        int writeIndex = request.getWriteDataIndex();
        if (writeIndex <= request.getWriteTarget().length - 1) {
            byteBuffer.put(request.getWriteTarget()[writeIndex]);
            request.setWriteDataIndex(writeIndex + 1);
        } else if (writeIndex == request.getWriteTarget().length)
            request.setWritePhase(toPhase);
    }

    public static void callback(Response msgResponse, PubSubCallBack pubSubCallBack) {
        List<byte[]> reply = Arrays.asList(msgResponse.getData());
        byte[] type = reply.get(0);
        String typeStr = StringEncoder.getString(type);
        int subscribedChannels;
        String strchannel;
        String msg;
        String bpattern;
        if (typeStr.equals(Operation.Keyword.SUBSCRIBE)) {
            subscribedChannels = buildLong(reply.get(2)).intValue();
            strchannel = StringEncoder.getString(reply.get(1));
            pubSubCallBack.onSubscribe(strchannel, subscribedChannels);
        }else if(typeStr.equals(Operation.Keyword.UNSUBSCRIBE)){
            subscribedChannels = buildLong(reply.get(2)).intValue();
            strchannel = StringEncoder.getString(reply.get(1));
            pubSubCallBack.onUnsubscribe(strchannel, subscribedChannels);
        }else if(typeStr.equals(Operation.Keyword.MESSAGE)){
            strchannel = StringEncoder.getString(reply.get(1));
            msg=StringEncoder.getString(reply.get(2));
            pubSubCallBack.onMessage(strchannel, msg);
        }else if(typeStr.equals(Operation.Keyword.PMESSAGE)){
            bpattern=StringEncoder.getString(reply.get(1));
            strchannel = StringEncoder.getString(reply.get(2));
            msg=StringEncoder.getString(reply.get(3));
            pubSubCallBack.onPMessage(bpattern,strchannel,msg);
        }else if(typeStr.equals(Operation.Keyword.PSUBSCRIBE)){
            subscribedChannels=buildLong(reply.get(2)).intValue();
            strchannel = StringEncoder.getString(reply.get(1));
            pubSubCallBack.onPSubscribe(strchannel,subscribedChannels);
        }else if(typeStr.equals(Operation.Keyword.PUNSUBSCRIBE)){
            subscribedChannels=buildLong(reply.get(2)).intValue();
            strchannel = StringEncoder.getString(reply.get(1));
            pubSubCallBack.onPUnsubscribe(strchannel,subscribedChannels);
        }
    }
}

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
     * ��int��װ��byte array
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
                //ȡ���ϴεĽ���״̬
                loadSubContext(response);
                boolean complete = processResult(byteBuffer, response);
                //��ԭ������״̬
                loadPrimaryContext(response);
                response.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
                response.setResponseType(ResponseType.MultiReply);
                if (complete) {
                    int multiDataIndex = response.getMultiDataIndex();
                    if (multiDataIndex == response.getmLen() - 1) {
                        //���������һ��multi response��
                        return true;
                    } else {
                        //����index+1
                        response.setMultiDataIndex(multiDataIndex + 1);
                        //����dLenStrÿ�ζ���append������������Ҫ���
                        response.setdLenStr(new StringBuilder());
                        //��ԭsubContext��״̬��Ϊ������һ��sub response��׼��
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
     * ��sub context��load��operation��״̬
     *
     * @param response
     */
    private static void loadPrimaryContext(Response response) {
        response.getSubParseContext().setParseStatus(response.getParseStatus());
        response.getSubParseContext().setResponseType(response.getResponseType());
    }

    /**
     * ����sub context��operation�У���ʾ��ʼ������response
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
     * ��������
     *
     * @param byteBuffer
     * @param response
     * @return
     */
    private static boolean processBulkReply(ByteBuffer byteBuffer, Response response) {
        while (byteBuffer.position() < byteBuffer.limit()) {
            //����byte buffer��remaining��ʵ��������ݴ�С���Ա�
            //���ʵ��������ݵ�����������remaining��˵��read����û��read��ȫ����Ҫ����read
            //�����˵��remaining�е����ݣ���ȫ���������������Ĵ�С����ôֱ������copy���У�����next״̬
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
     * ��װwrite byteBuffer���涨����״̬��Ŀ�����ڣ���byte buffer��������£�д��socketchannel��������ÿһ��д����ȡ�
     * RAW:��ʼ״̬��byteBuffer��ʼд��Э���ʼ���Ǻţ�����д�������index����operation�������Ҫд�Ĳ������ȵ����ݣ���һ�׶ο�ʼд�����úõ����ݣ�ֱ��ת��WRITE_ARGS_LENGTH״̬
     * WRITE_ARGS_LENGTH����ʼд������������ݣ�ÿ��д��1 byte���������ݵ�operation�е�writeIndex�������Ƿ�д����ϣ����д�����ת��WRITE_ARGS_R
     * WRITE_ARGS_R:д�� '\r',д��ת��WRITE_ARGS_N
     * WRITE_ARGS_N:д�� '\n',д��ת��WRITE_DOLLAR
     * WRITE_DOLLAR:д�� '$',ͬʱ׼����һ��д�������command�����Ƴ��ȣ�����operation��writeTarget��writeIndex��ת��WRITE_COMMAND_LENGTH
     * WRITE_COMMAND_LENGTH:byte bufferд��command���ƣ�ÿbyteд��byte buffer��д�����ת��WRITE_COMMAND_LENGTH_R
     * WRITE_COMMAND_LENGTH_R:д��'\r',��ɺ�ת��WRITE_COMMAND_LENGTH_N
     * WRITE_COMMAND_LENGTH_N:д��'\n',׼��д��command�����Ƶ����ݣ����ת��WRITE_COMMAND
     * WRITE_COMMAND:д��byte buffer command���Ƶ�byte ���ݣ�ÿ��һ��byte��д�����ת��WRITE_COMMAND_R
     * WRITE_COMMAND_R:д��'\r',д��ת��WRITE_COMMAND_N
     * WRITE_COMMAND_N:д��'\n',д��ת��WRITE_ARGS
     * WRITE_ARGS:���״̬�������Ƿ��в����������Ƿ��˳�fill command�����û�в�������ôbyte buffer�������ϣ�����в�����ô��ת��WRITE_ARGS_DOLLAR
     * WRITE_ARGS_DOLLAR:д��'$',׼������byte����������,ת��WRITE_ARG_LENGTH
     * WRITE_ARG_LENGTH:��ʼд�뵱ǰ����byte���������ݣ�д��ת��WRITE_ARG_LENGTH_R
     * WRITE_ARG_LENGTH_R:д��'\r',ת��WRITE_ARG_LENGTH_N
     * WRITE_ARG_LENGTH_N:д��'\n',׼����ǰ����byte���ݣ�ת��WRITE_ARG
     * WRITE_ARG:д�뵱ǰ����byte���ݣ�һ��д��1byte��д��ת��WRITE_ARG_R
     * WRITE_ARG_R:д��'\r',ת��WRITE_ARG_N
     * WRITE_ARG_N:д��'\n',ת��WRITE_ARGS
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
                    //����û��break�������WRITE_ARGS��Ϊ�˷������
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
                    request.setWriteArgsIndex(request.getWriteArgsIndex() + 1);//����д����һ������������м���д��û�еĻ���WRITE_ARGS����true
                    request.setWritePhase(WritePhase.WRITE_ARGS);
                    break;
            }
        }
        return false;
    }

    /**
     * ����Ҫд�����ݣ�index������operation��next״̬
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
     * д����per byte buffer
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

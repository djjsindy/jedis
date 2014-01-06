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
     * ��int��װ��byte array
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
                //ȡ���ϴεĽ���״̬
                loadSubContext(operation);
                boolean complete=processResult(byteBuffer,operation);
                //��ԭ������״̬
                loadPrimaryContext(operation);
                operation.setParseStatus(ParseStatus.READ_RESULT_LENGTH_N);
                operation.setResponseType(ResponseType.MultiReply);
                if(complete){
                    int multiDataIndex=operation.getMutilDataIndex();
                    if(multiDataIndex==operation.getmLen()-1){
                        //���������һ��multi response��
                        return true;
                    }else{
                        //����index+1
                        operation.setMutilDataIndex(multiDataIndex+1);
                        //����dLenStrÿ�ζ���append������������Ҫ���
                        operation.setdLenStr(new StringBuilder());
                        //��ԭsubContext��״̬��Ϊ������һ��sub response��׼��
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
     * ��sub context��load��operation��״̬
     * @param operation
     */
    private static void loadPrimaryContext(Operation operation) {
        operation.getSubParseContext().setParseStatus(operation.getParseStatus());
        operation.getSubParseContext().setResponseType(operation.getResponseType());
    }

    /**
     * ����sub context��operation�У���ʾ��ʼ������response
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
     * ��������
     * @param byteBuffer
     * @param operation
     * @return
     */
    private static boolean processBulkReply(ByteBuffer byteBuffer,Operation operation) {
        while(byteBuffer.position()<byteBuffer.limit()){
            //����byte buffer��remaining��ʵ��������ݴ�С���Ա�
            //���ʵ��������ݵ�����������remaining��˵��read����û��read��ȫ����Ҫ����read
            //�����˵��remaining�е����ݣ���ȫ���������������Ĵ�С����ôֱ������copy���У�����next״̬
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
                    //����û��break�������WRITE_ARGS��Ϊ�˷������
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
                    operation.setWriteArgsIndex(operation.getWriteArgsIndex()+1);//����д����һ������������м���д��û�еĻ���WRITE_ARGS����true
                    operation.setWritePhase(WritePhase.WRITE_ARGS);
                    break;
            }
        }
        return false;
    }

    /**
     * ����Ҫд�����ݣ�index������operation��next״̬
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
     * д����per byte buffer
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

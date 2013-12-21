package com.sohu.redis.operation;

import com.sohu.redis.protocol.ParseStatus;
import com.sohu.redis.protocol.RedisProtocol;
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
     * ����response�ĳ�ʼ״̬
     */
    private ParseStatus parseStatus=ParseStatus.RAW;

    /**
     * ��װrequest�ĳ�ʼ״̬
     */
    private WritePhase writePhase=WritePhase.RAW;

    /**
     * ��ǰoperation��Ҫд�����ݣ�����key��byte[],value��byte[]��
     */
    private byte[] writeTarget;

    /**
     * дwriteTarget��index�������ǰwrite��buffer������Ҫwrite�����磬index����д������
     */
    private int writeDataIndex;

    /**
     * ��¼���������index
     */
    private int writeArgsIndex;

    /**
     * ��ȡmulti����response��index
     */
    private int multiDataIndex;


    /**
     * ���������У�response���ݵĳ��ȣ�get�����ȣ���¼�м����ݣ�
     */
    private StringBuilder dLenStr=new StringBuilder();

    /**
     * ���ز����У���¼����ĸ�������¼�м������
     */
    private StringBuilder mLenstr=new StringBuilder();

    /**
     * ��¼response���ݣ���ά������Ϊ�˼�¼mget�࣬������ؽ��������
     */
    private byte[][] data;

    /**
     * ��¼response�Ƿ��׳����쳣
     */
    private boolean exception;

    /**
     * data ʣ�����byteδ��ȡ
     */
    private int dLast;

    /**
     * response��type
     */
    private ResponseType responseType;

    /**
     * request��command
     */
    private Command command;

    /**
     * ��¼request����
     */
    private byte[][] args;

    /**
     * ����response���ݵ�bufsize����������ˣ�2������
     */
    private static int DATABUF_SIZE=1024;

    /**
     *response��data buf��index
     */
    private int dataIndex;

    /**
     * ���ز����У���¼response�У��������
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

    public StringBuilder getmLenstr() {
        return mLenstr;
    }

    public void setmLenstr(StringBuilder mLenstr) {
        this.mLenstr = mLenstr;
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


    public enum Command {
        GET, SET,SETEX;
    }

    /**
     * ��ͨbulk multi bulk�����������ݵĻص�������
     * @param byteBuffer
     * @param length д�����ݵĳ���
     */
    public void addData(ByteBuffer byteBuffer,int length){
        if(byteBuffer==null){
            return;
        }
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
        byteBuffer.get(temp,offset,length);
        data[multiDataIndex]=temp;
    }

    /**
     * ����status code���쳣��Ϣ�Ļص�������һ��byteһ��byte��д
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
     * ������ȫ�����ݣ�ȥ��data�����Ŀ�����
     */
    public void compactMsgData(){
        byte[] temp=new byte[dataIndex];
        System.arraycopy(data[multiDataIndex],0,temp,0,dataIndex);
        data[multiDataIndex]=temp;
    }

    /**
     * ���ݽ�����ϣ�tcpcomponent�̰߳�����set��future����
     */
    public void pushData(){
        this.future.setResult(data);
    }


}

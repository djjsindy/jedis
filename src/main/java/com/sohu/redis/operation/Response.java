package com.sohu.redis.operation;

import com.sohu.redis.protocol.ParseStatus;
import com.sohu.redis.protocol.SubParseContext;

import java.nio.ByteBuffer;

/**
 * Created by jianjundeng on 1/6/14.
 */
public class Response {

    /**
     * ����response�ĳ�ʼ״̬
     */
    private ParseStatus parseStatus=ParseStatus.RAW;

    /**
     * �����Ӳ�����״̬�����𱣳�subParseContext��operation���״̬��ת��
     */
    private SubParseContext subParseContext=new SubParseContext();

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
    private StringBuilder mLenStr=new StringBuilder();

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

    public ParseStatus getParseStatus() {
        return parseStatus;
    }

    public void setParseStatus(ParseStatus parseStatus) {
        this.parseStatus = parseStatus;
    }

    public SubParseContext getSubParseContext() {
        return subParseContext;
    }

    public void setSubParseContext(SubParseContext subParseContext) {
        this.subParseContext = subParseContext;
    }

    public int getMultiDataIndex() {
        return multiDataIndex;
    }

    public void setMultiDataIndex(int multiDataIndex) {
        this.multiDataIndex = multiDataIndex;
    }

    public StringBuilder getdLenStr() {
        return dLenStr;
    }

    public void setdLenStr(StringBuilder dLenStr) {
        this.dLenStr = dLenStr;
    }

    public StringBuilder getmLenStr() {
        return mLenStr;
    }

    public void setmLenStr(StringBuilder mLenStr) {
        this.mLenStr = mLenStr;
    }

    public byte[][] getData() {
        return data;
    }

    public void setData(byte[][] data) {
        this.data = data;
    }

    public boolean isException() {
        return exception;
    }

    public void setException(boolean exception) {
        this.exception = exception;
    }

    public int getdLast() {
        return dLast;
    }

    public void setdLast(int dLast) {
        this.dLast = dLast;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public void setResponseType(ResponseType responseType) {
        this.responseType = responseType;
    }

    public int getDataIndex() {
        return dataIndex;
    }

    public void setDataIndex(int dataIndex) {
        this.dataIndex = dataIndex;
    }

    public int getmLen() {
        return mLen;
    }

    public void setmLen(int mLen) {
        this.mLen = mLen;
    }

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

    public void compactMsgData(){
        byte[] temp=new byte[dataIndex];
        System.arraycopy(data[multiDataIndex],0,temp,0,dataIndex);
        data[multiDataIndex]=temp;
    }

    public void clear(){
        parseStatus=ParseStatus.RAW;
        subParseContext.clear();
        multiDataIndex=0;
        dLenStr=new StringBuilder();
        mLenStr=new StringBuilder();
        data=null;
        dLast=0;
        responseType=null;
        dataIndex=0;
        mLen=0;
    }
}

package com.sohu.redis.protocol;

import com.sohu.redis.operation.ResponseType;

/**
 * �������multi response�У��Ӳ�������ʱ״̬
 * Created by jianjundeng on 12/23/13.
 */
public class SubParseContext {

    /**
     * ����multi response�õ����ݴ��ӽ���״̬
     */
    private ParseStatus parseStatus=ParseStatus.RAW;

    /**
     * ���������У�response���ݵĳ��ȣ�get�����ȣ���¼�м����ݣ�
     */
    private StringBuilder dLenStr=new StringBuilder();

    /**
     * ����multi response���ݴ��ӽ�������
     */
    private ResponseType responseType;


    public StringBuilder getdLenStr() {
        return dLenStr;
    }

    public void setdLenStr(StringBuilder dLenStr) {
        this.dLenStr = dLenStr;
    }

    public ParseStatus getParseStatus() {
        return parseStatus;
    }

    public void setParseStatus(ParseStatus parseStatus) {
        this.parseStatus = parseStatus;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public void setResponseType(ResponseType responseType) {
        this.responseType = responseType;
    }
}

package com.sohu.redis.protocol;

import com.sohu.redis.operation.ResponseType;

/**
 * 保存解析multi response中，子操作的临时状态
 * Created by jianjundeng on 12/23/13.
 */
public class SubParseContext {

    /**
     * 解析multi response用到的暂存子解析状态
     */
    private ParseStatus parseStatus=ParseStatus.RAW;

    /**
     * 解析multi response，暂存子解析类型
     */
    private ResponseType responseType;

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

    public void clear(){
        parseStatus=ParseStatus.RAW;
        responseType=null;
    }
}

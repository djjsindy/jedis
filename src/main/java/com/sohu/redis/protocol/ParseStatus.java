package com.sohu.redis.protocol;

/**
 * Created by jianjundeng on 12/18/13.
 */
public enum ParseStatus {
    RAW,READLENGTH,MEETLENGTHR,MEETLENGTHN,DATAEND,MEETDATAR,  //bulk
    READ_MSG,MSG_R,                //simple
    READ_RESULT_LENGTH,READ_RESULT_LENGTH_R,READ_RESULT_LENGTH_N, //multi
}

package com.sohu.redis.protocol;

/**
 * Created by jianjundeng on 12/18/13.
 */
public enum ParseStatus {
    RAW,
    READ_LENGTH,
    READ_LENGTH_R,
    READ_LENGTH_N,
    READ_DATA_END,
    READ_DATA_R,  //bulk

    READ_MSG,MSG_R,//simple


    READ_RESULT_LENGTH,
    READ_RESULT_LENGTH_R,
    READ_RESULT_LENGTH_N, //multi
}

package com.sohu.redis.protocol;

/**
 * Created by jianjundeng on 12/19/13.
 */
public enum WritePhase {
    RAW,
    WRITE_ARGS_LENGTH,
    WRITE_ARGS_R,
    WRITE_ARGS_N,
    WRITE_DOLLAR,
    WRITE_COMMAND_LENGTH,
    WRITE_COMMAND_LENGTH_R,
    WRITE_COMMAND_LENGTH_N,
    WRITE_COMMAND,
    WRITE_COMMAND_R,
    WRITE_COMMAND_N,
    WRITE_ARGS,
    WRITE_ARGS_DOLLAR,
    WRITE_ARG_LENGTH,
    WRITE_ARG_LENGTH_R,
    WRITE_ARG_LENGTH_N,
    WRITE_ARG,
    WRITE_ARG_R,
    WRITE_ARG_N,
}

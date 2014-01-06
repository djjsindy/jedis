package com.sohu.redis.operation;

import com.sohu.redis.protocol.WritePhase;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jianjundeng on 1/6/14.
 */
public class Request {
    /**
     * 组装request的初始状态
     */
    private WritePhase writePhase=WritePhase.RAW;

    /**
     * 当前operation需要写的数据，比如key的byte[],value的byte[]等
     */
    private byte[] writeTarget;

    /**
     * 写writeTarget的index，如果当前write的buffer满了需要write到网络，index保存写的索引
     */
    private int writeDataIndex;

    /**
     * 记录多个参数的index
     */
    private int writeArgsIndex;

    /**
     * request的command
     */
    private Operation.Command command;

    /**
     * 记录request参数
     */
    private byte[][] args;

    public Operation.Command getCommand() {
        return command;
    }

    public void setCommand(Operation.Command command) {
        this.command = command;
    }

    public byte[][] getArgs() {
        return args;
    }

    public void setArgs(byte[][] args) {
        this.args = args;
    }

    public void addArgs(List<byte[]> newArgs){
        List<byte[]> list= Arrays.asList(args);
        list.addAll(newArgs);
        byte[][] temp=new byte[list.size()][];
        args=list.toArray(temp);
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
}

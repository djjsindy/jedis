package com.sohu.redis.operation;

import com.sohu.redis.protocol.WritePhase;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jianjundeng on 1/6/14.
 */
public class Request {
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
     * request��command
     */
    private Operation.Command command;

    /**
     * ��¼request����
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

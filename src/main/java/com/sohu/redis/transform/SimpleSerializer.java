package com.sohu.redis.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by jianjundeng on 12/17/13.
 */
public class SimpleSerializer implements Serializer{

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSerializer.class);

    public  byte[] encode(Object object){
        try {
            ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream=new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("object encode error");
        }
        return null;
    }

    public  Object decode(byte[] b){
        try {
            ByteArrayInputStream byteArrayInputStream=new ByteArrayInputStream(b);
            ObjectInputStream objectInputStream=new ObjectInputStream(byteArrayInputStream);
            return objectInputStream.readObject();
        } catch (IOException e) {
            LOGGER.error("object decode error");
        } catch (ClassNotFoundException e) {
            LOGGER.error("object decode error");
        }
        return null;
    }
}

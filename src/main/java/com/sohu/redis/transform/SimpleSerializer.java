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
        ObjectOutputStream objectOutputStream = null;
        try {
            ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();
            objectOutputStream=new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("object encode error");
        }finally {
            try {
                objectOutputStream.close();
            } catch (IOException e) {
                LOGGER.error("close output stream error");
            }
        }
        return null;
    }

    public  Object decode(byte[] b){
        ByteArrayInputStream byteArrayInputStream=null;
        try {
            byteArrayInputStream=new ByteArrayInputStream(b);
            ObjectInputStream objectInputStream=new ObjectInputStream(byteArrayInputStream);
            return objectInputStream.readObject();
        } catch (IOException e) {
            LOGGER.error("object decode error");
        } catch (ClassNotFoundException e) {
            LOGGER.error("object decode error");
        }finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {
                LOGGER.error("close input stream error");
            }
        }
        return null;
    }
}

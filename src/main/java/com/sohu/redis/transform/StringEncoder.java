package com.sohu.redis.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Created by jianjundeng on 12/17/13.
 */
public class StringEncoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringEncoder.class);

    private static final String DEFAULT_CHARSET="UTF-8";

    public static String getString(byte[] b){
        try {
            return new String(b,DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("string encode error");
        }
        return null;
    }

    public static byte[] getBytes(String s){
        try {
            return s.getBytes(DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("string encode error");
        }
        return null;
    }
}

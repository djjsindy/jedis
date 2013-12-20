package com.sohu.redis.transform;

/**
 * Created by jianjundeng on 12/17/13.
 */
public interface Serializer {

    public byte[] encode(Object object);

    public Object decode(byte[] b);
}

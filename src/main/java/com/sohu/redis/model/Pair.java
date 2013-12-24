package com.sohu.redis.model;

/**
 * Created by jianjundeng on 12/24/13.
 */
public class Pair{

    private String key;

    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Pair(String key,String value){
        this.key=key;
        this.value=value;
    }
}

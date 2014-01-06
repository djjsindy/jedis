package com.sohu.redis.model;

import com.sohu.redis.operation.Operation;
import com.sohu.redis.transform.StringEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianjundeng on 1/6/14.
 */
public class SortingParams {

    private List<byte[]> params = new ArrayList<byte[]>();


    public SortingParams by(String pattern) {
        return by(StringEncoder.getBytes(pattern));
    }


    public SortingParams by(byte[] pattern) {
        params.add(Operation.Keyword.BY.getBytes());
        params.add(pattern);
        return this;
    }


    public SortingParams nosort() {
        params.add(Operation.Keyword.BY.getBytes());
        params.add(Operation.Keyword.NOSORT.getBytes());
        return this;
    }

    public List<byte[]> getParams() {
        return params;
    }


    public SortingParams desc() {
        params.add(Operation.Keyword.DESC.getBytes());
        return this;
    }


    public SortingParams asc() {
        params.add(Operation.Keyword.ASC.getBytes());
        return this;
    }


    public SortingParams limit(int start,int count) {
        params.add(Operation.Keyword.LIMIT.getBytes());
        params.add(StringEncoder.getBytes(Integer.toString(start)));
        params.add(StringEncoder.getBytes(Integer.toString(count)));
        return this;
    }


    public SortingParams alpha() {
        params.add(Operation.Keyword.ALPHA.getBytes());
        return this;
    }


    public SortingParams get(String... patterns) {
        for (final String pattern : patterns) {
            params.add(Operation.Keyword.GET.getBytes());
            params.add(StringEncoder.getBytes(pattern));
        }
        return this;
    }


    public SortingParams get(byte[]... patterns) {
        for (final byte[] pattern : patterns) {
            params.add(Operation.Keyword.GET.getBytes());
            params.add(pattern);
        }
        return this;
    }
}

package com.sohu.redis.operation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jianjundeng on 12/16/13.
 */
public class OperationFuture implements Future {

    private Object result;

    private Condition condition;

    private ReentrantLock reentrantLock;

    public OperationFuture() {
        reentrantLock = new ReentrantLock();
        condition = reentrantLock.newCondition();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return result == null;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        try {
            reentrantLock.lock();
            condition.await();
        } finally {
            reentrantLock.unlock();
        }
        return result;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            reentrantLock.lock();
            condition.await(timeout, unit);
        } finally {
            reentrantLock.unlock();
        }
        return result;
    }

    public void setResult(Object result) {
        try {
            reentrantLock.lock();
            this.result = result;
            condition.signal();
        } finally {
            this.reentrantLock.unlock();
        }
    }
}

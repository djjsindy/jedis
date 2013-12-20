package com.sohu.redis.net;

/**
 * Created by jianjundeng on 12/17/13.
 */
public class Event {

    private EventType eventType;

    private RedisConnection redisConnection;

    public Event(EventType eventType,RedisConnection redisConnection){
        this.eventType=eventType;
        this.redisConnection=redisConnection;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public RedisConnection getRedisConnection() {
        return redisConnection;
    }

    public void setRedisConnection(RedisConnection redisConnection) {
        this.redisConnection = redisConnection;
    }
}

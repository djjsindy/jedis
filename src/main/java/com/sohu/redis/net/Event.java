package com.sohu.redis.net;

/**
 * Created by jianjundeng on 12/17/13.
 */
public class Event {

    private EventType eventType;

    private Connection connection;

    public Event(EventType eventType,Connection connection){
        this.eventType=eventType;
        this.connection=connection;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection Connection) {
        this.connection = Connection;
    }
}

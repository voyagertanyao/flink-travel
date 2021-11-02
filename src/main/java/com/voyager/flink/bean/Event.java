package com.voyager.flink.bean;

public class Event {
    private String id;
    private String type;
    private long duration;
    private long start;

    public Event(String id, String type, long duration, long start) {
        this.id = id;
        this.type = type;
        this.duration = duration;
        this.start = start;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", duration=" + duration +
                ", start=" + start +
                '}';
    }
}

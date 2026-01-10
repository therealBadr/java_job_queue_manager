package com.jobqueue.core;

import com.google.gson.Gson;
import java.util.UUID;

/**
 * Abstract base class for job implementations providing common functionality
 */
public abstract class BaseJob implements Job {
    private static final Gson gson = new Gson();
    
    private final String id;
    private String payload;
    private int priority = 0;
    private int maxRetries = 3;

    protected BaseJob() {
        this.id = UUID.randomUUID().toString();
    }

    /**
     * Constructor for creating a job with specific parameters
     * Used by Scheduler when instantiating jobs from database
     */
    protected BaseJob(String id, String payload, int priority) {
        this.id = id;
        this.payload = payload;
        this.priority = priority;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getType() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getPayload() {
        return payload;
    }

    protected void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    protected void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

    protected void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @Override
    public boolean isCancellable() {
        return true;
    }

    protected <T> String toPayload(T data) {
        return gson.toJson(data);
    }

    protected <T> T fromPayload(String payload, Class<T> clazz) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        return gson.fromJson(payload, clazz);
    }
}

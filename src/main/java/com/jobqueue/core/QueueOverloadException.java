package com.jobqueue.core;

public class QueueOverloadException extends RuntimeException {
    
    private final int currentDepth;
    private final int threshold;
    
    public QueueOverloadException(String message) {
        super(message);
        this.currentDepth = -1;
        this.threshold = -1;
    }
    
    public QueueOverloadException(String message, int currentDepth, int threshold) {
        super(message);
        this.currentDepth = currentDepth;
        this.threshold = threshold;
    }
    
    public QueueOverloadException(String message, Throwable cause) {
        super(message, cause);
        this.currentDepth = -1;
        this.threshold = -1;
    }
    
    public int getCurrentDepth() {
        return currentDepth;
    }
    
    public int getThreshold() {
        return threshold;
    }
}

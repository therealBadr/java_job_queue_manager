package com.jobqueue.core;

/**
 * Exception thrown when the job queue is overloaded and cannot accept new jobs.
 * This occurs when backpressure is activated due to excessive queue depth.
 */
public class QueueOverloadException extends RuntimeException {
    
    private final int currentDepth;
    private final int threshold;
    
    /**
     * Create a new QueueOverloadException.
     * 
     * @param message the error message
     */
    public QueueOverloadException(String message) {
        super(message);
        this.currentDepth = -1;
        this.threshold = -1;
    }
    
    /**
     * Create a new QueueOverloadException with queue depth information.
     * 
     * @param message the error message
     * @param currentDepth the current queue depth
     * @param threshold the queue depth threshold
     */
    public QueueOverloadException(String message, int currentDepth, int threshold) {
        super(message);
        this.currentDepth = currentDepth;
        this.threshold = threshold;
    }
    
    /**
     * Create a new QueueOverloadException with a cause.
     * 
     * @param message the error message
     * @param cause the underlying cause
     */
    public QueueOverloadException(String message, Throwable cause) {
        super(message, cause);
        this.currentDepth = -1;
        this.threshold = -1;
    }
    
    /**
     * Get the current queue depth at the time of the exception.
     * 
     * @return the current queue depth, or -1 if not available
     */
    public int getCurrentDepth() {
        return currentDepth;
    }
    
    /**
     * Get the queue depth threshold.
     * 
     * @return the threshold, or -1 if not available
     */
    public int getThreshold() {
        return threshold;
    }
}

package com.jobqueue.core;

/**
 * Exception thrown when the job queue is overloaded and cannot accept new jobs.
 * 
 * <p>This exception is thrown as a backpressure mechanism to prevent system overload.
 * When the pending job queue depth exceeds the configured threshold (default: 1000 jobs),
 * the scheduler activates backpressure and rejects new job submissions.</p>
 * 
 * <p><b>Design Rationale:</b></p>
 * <ul>
 *   <li>Prevents memory exhaustion from unbounded queue growth</li>
 *   <li>Provides early feedback to clients that the system is overloaded</li>
 *   <li>Allows clients to implement retry logic or load shedding</li>
 *   <li>Protects database from excessive writes</li>
 * </ul>
 * 
 * <p><b>Recovery:</b> Backpressure is automatically released when queue depth
 * drops below 80% of the threshold. Clients should retry after a delay.</p>
 * 
 * <p><b>Example Handling:</b></p>
 * <pre>{@code
 * try {
 *     scheduler.submitJob(job);
 * } catch (QueueOverloadException e) {
 *     logger.warn("Queue overloaded: " + e.getCurrentDepth() + 
 *                 "/" + e.getThreshold());
 *     // Implement retry with exponential backoff
 *     Thread.sleep(5000);
 *     scheduler.submitJob(job); // Retry
 * }
 * }</pre>
 * 
 * @author Job Queue Team
 * @see Scheduler#submitJob(Job)
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

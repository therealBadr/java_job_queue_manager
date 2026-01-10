package com.jobqueue.core;

/**
 * Job interface representing a unit of work in the queue system.
 * All job implementations must provide these core methods to enable
 * proper job lifecycle management, execution, and monitoring.
 */
public interface Job {
    
    /**
     * Get the unique identifier for this job.
     * The ID should be a UUID string (36 characters) that uniquely identifies
     * this job instance across the entire system.
     * 
     * @return the job's unique identifier as a String (UUID format)
     */
    String getId();
    
    /**
     * Get the type/class name of this job.
     * This is typically the simple class name (e.g., "EmailJob", "ReportJob")
     * and is used for job categorization, logging, and routing to appropriate handlers.
     * 
     * @return the job type as a String
     */
    String getType();
    
    /**
     * Execute the job's business logic.
     * This method contains the actual work that needs to be performed.
     * Implementations should be idempotent when possible to safely handle retries.
     * 
     * @param context the execution context containing job metadata, worker info,
     *                and utilities for logging and state management
     * @throws Exception if job execution fails. The exception will be caught
     *                   by the worker and may trigger a retry based on retry policy.
     */
    void execute(JobContext context) throws Exception;
    
    /**
     * Get the job's payload data.
     * The payload is typically a JSON string containing all the data needed
     * to execute the job. This allows jobs to be serialized to the database
     * and deserialized for execution.
     * 
     * @return the job payload as a JSON string, or null if no payload is needed
     */
    String getPayload();
    
    /**
     * Get the priority level of this job.
     * Higher numbers indicate higher priority. Jobs with higher priority
     * should be executed before lower priority jobs when multiple jobs are queued.
     * Default priority is typically 0 for normal jobs.
     * 
     * @return the priority level as an integer (higher = more important)
     */
    int getPriority();
    
    /**
     * Get the maximum number of retry attempts allowed for this job.
     * If a job fails, it will be retried up to this many times before being
     * marked as permanently failed and potentially moved to the dead letter queue.
     * 
     * @return the maximum number of retries (0 means no retries, 3 is typical)
     */
    int getMaxRetries();
    
    /**
     * Determine if this job can be cancelled while running.
     * Some jobs perform critical operations that should not be interrupted,
     * while others can be safely cancelled. This method allows job implementations
     * to declare their cancellation policy.
     * 
     * @return true if the job can be cancelled mid-execution, false otherwise
     */
    boolean isCancellable();
}

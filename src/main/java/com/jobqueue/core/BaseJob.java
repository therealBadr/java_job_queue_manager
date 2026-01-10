package com.jobqueue.core;

import com.google.gson.Gson;
import java.util.UUID;

/**
 * Abstract base class for job implementations providing common functionality.
 * 
 * <p>This class handles:</p>
 * <ul>
 *   <li>UUID generation for job IDs</li>
 *   <li>Priority and retry configuration</li>
 *   <li>JSON serialization/deserialization of payloads</li>
 *   <li>Default cancellation policy</li>
 * </ul>
 * 
 * <p><b>Design Pattern:</b> This is a Template Method pattern - concrete job types
 * extend this class and implement the execute() method while inheriting common behavior.</p>
 * 
 * <p><b>Thread Safety:</b> Job instances are not shared between threads. Each worker
 * gets its own job instance, so no synchronization is needed.</p>
 * 
 * <p><b>Usage:</b></p>
 * <pre>{@code
 * public class MyJob extends BaseJob {
 *     public void execute(JobContext context) throws Exception {
 *         MyData data = fromPayload(getPayload(), MyData.class);
 *         // ... process data
 *     }
 * }
 * }</pre>
 * 
 * @author Job Queue Team
 * @see Job
 */
public abstract class BaseJob implements Job {
    // Shared Gson instance for JSON serialization - thread-safe
    private static final Gson gson = new Gson();
    
    private final String id;
    private String payload;
    private int priority = 0;      // Default priority: normal
    private int maxRetries = 3;    // Default max retries

    /**
     * Default constructor - generates a new UUID for the job ID.
     * 
     * <p>Use this constructor when creating new jobs to submit to the queue.</p>
     */
    protected BaseJob() {
        this.id = UUID.randomUUID().toString();
    }

    /**
     * Constructor for creating a job with specific parameters.
     * 
     * <p>This constructor is used by the Scheduler when instantiating jobs
     * from database records. The Scheduler reads job data from the database
     * and reconstructs the Job object with the stored ID, payload, and priority.</p>
     * 
     * <p><b>Why this exists:</b> Jobs need to be persisted and reconstructed
     * with their original IDs for tracking and idempotency.</p>
     * 
     * @param id the job's UUID (from database)
     * @param payload the serialized job data (JSON string)
     * @param priority the job's priority level
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
        // Return simple class name for job type identification
        return this.getClass().getSimpleName();
    }

    @Override
    public String getPayload() {
        return payload;
    }

    /**
     * Set the job's payload.
     * 
     * <p>Typically called by job-specific setter methods that convert
     * domain objects to JSON strings.</p>
     * 
     * @param payload JSON string representation of job data
     */
    protected void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    /**
     * Set the job's priority level.
     * 
     * <p>Higher numbers = higher priority. Typical range: 0-10</p>
     * 
     * @param priority the priority level (0 = normal, 10 = highest)
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the maximum number of retry attempts.
     * 
     * <p>Jobs that fail will be retried up to this many times with
     * exponential backoff (2^retry_count minutes).</p>
     * 
     * @param maxRetries the maximum retry count (0 = no retries)
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @Override
    public boolean isCancellable() {
        // Default: all jobs can be cancelled
        // Override this method to return false for jobs that should not be interrupted
        return true;
    }

    /**
     * Convert a domain object to JSON payload string.
     * 
     * <p>This is a utility method for job implementations to serialize
     * their data before setting the payload.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>{@code
     * EmailData data = new EmailData(recipient, subject, body);
     * setPayload(toPayload(data));
     * }</pre>
     * 
     * @param data the object to serialize
     * @param <T> the type of the object
     * @return JSON string representation
     */
    protected <T> String toPayload(T data) {
        return gson.toJson(data);
    }

    /**
     * Deserialize JSON payload to a domain object.
     * 
     * <p>This is a utility method for job implementations to deserialize
     * their data from the payload string.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>{@code
     * EmailData data = fromPayload(getPayload(), EmailData.class);
     * sendEmail(data.recipient, data.subject, data.body);
     * }</pre>
     * 
     * @param payload the JSON string to deserialize
     * @param clazz the class of the target object
     * @param <T> the type of the object
     * @return the deserialized object, or null if payload is null/empty
     */
    protected <T> T fromPayload(String payload, Class<T> clazz) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        return gson.fromJson(payload, clazz);
    }
}

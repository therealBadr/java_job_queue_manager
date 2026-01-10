package com.jobqueue.core;

import com.jobqueue.db.JobRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Context information for job execution providing execution environment and utilities.
 * 
 * <p>This class serves as the bridge between a job's business logic and the scheduler framework.
 * It provides:</p>
 * <ul>
 *   <li>Persistent logging to the database</li>
 *   <li>Cancellation checking via atomic flag</li>
 *   <li>Metadata storage for passing data between retries</li>
 *   <li>Progress tracking for long-running jobs</li>
 * </ul>
 * 
 * <p><b>Thread Safety:</b> This class is thread-safe. The cancellation flag uses AtomicBoolean
 * for atomic read/write operations, and the metadata map is synchronized internally.</p>
 * 
 * <p><b>Usage Pattern:</b></p>
 * <pre>{@code
 * public void execute(JobContext context) throws Exception {
 *     context.log("INFO", "Starting job execution");
 *     for (int i = 0; i < 100; i++) {
 *         context.throwIfCancelled(); // Check for cancellation
 *         processItem(i);
 *         if (i % 10 == 0) {
 *             context.updateProgress("Processed " + i + " items");
 *         }
 *     }
 * }
 * }</pre>
 * 
 * @author Job Queue Team
 * @see Job#execute(JobContext)
 */
public class JobContext {
    private final String jobId;
    private final JobRepository repository;
    // Atomic flag for thread-safe cancellation checking
    private final AtomicBoolean cancelled;
    // Thread-safe metadata storage for job state (ConcurrentHashMap)
	private final Map<String, Object> metadata;

    /**
     * Create a new job context for execution.
     * 
     * <p>This constructor is called by the Worker when a job begins execution.
     * The context is passed to the job's execute() method.</p>
     * 
     * @param jobId the unique identifier of the job being executed
     * @param repository the job repository for database operations (logging, state updates)
     */
    public JobContext(String jobId, JobRepository repository) {
        this.jobId = jobId;
        this.repository = repository;
        // Initialize cancellation flag to false - job starts in non-cancelled state
        this.cancelled = new AtomicBoolean(false);
        // Use ConcurrentHashMap for lock-free thread-safe metadata access
		this.metadata = new ConcurrentHashMap<>();
	}

    /**
     * Get the unique identifier of the job being executed.
     * 
     * @return the job's UUID
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Log a message to the job_logs table for audit and debugging purposes.
     * 
     * <p><b>Database Transaction:</b> Each log entry is written in its own transaction.
     * If logging fails (e.g., connection error), the error is logged to stderr
     * but does not fail the job - logging failures should not cause job failures.</p>
     * 
     * <p><b>Concurrency:</b> Multiple workers can log simultaneously. Each log entry
     * is independent and uses its own database connection from the pool.</p>
     * 
     * @param level the log level (INFO, WARN, ERROR, DEBUG) - used for filtering
     * @param message the log message - should be descriptive for troubleshooting
     */
    public void log(String level, String message) {
        String sql = "INSERT INTO job_logs (job_id, timestamp, level, message) VALUES (?, ?, ?, ?)";
        
        // Use try-with-resources for automatic connection cleanup
        try (Connection conn = repository.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, level);
            stmt.setString(4, message);
            
            stmt.executeUpdate();
        } catch (SQLException e) {
            // Log to system logger as fallback - don't propagate exception
            // Why: Logging failures should not cause job failures
            System.err.println("Failed to log to database: " + e.getMessage());
        }
    }

    /**
     * Update the job's progress status message.
     * This is a convenience method that logs at INFO level.
     * 
     * <p>Use this for progress updates in long-running jobs to provide
     * visibility into execution status.</p>
     * 
     * @param status the progress message (e.g., "Processed 50/100 items")
     */
    public void updateProgress(String status) {
        log("INFO", status);
    }

    /**
     * Check if the job has been cancelled.
     * 
     * <p><b>Thread Safety:</b> Uses AtomicBoolean for lock-free, atomic read.
     * This method can be called from the job thread while another thread
     * (e.g., scheduler) calls cancel().</p>
     * 
     * @return true if the job was cancelled, false if still running normally
     */
    public boolean isCancelled() {
        return cancelled.get(); // Atomic read
    }

    /**
     * Mark this job as cancelled.
     * 
     * <p><b>Thread Safety:</b> This method is typically called by the scheduler
     * while the job is executing in a worker thread. The atomic flag ensures
     * the cancellation is immediately visible to the worker thread.</p>
     * 
     * <p><b>Note:</b> Setting this flag does not immediately stop execution.
     * The job must call throwIfCancelled() to actually check and respond to cancellation.</p>
     */
    public void cancel() {
        cancelled.set(true); // Atomic write
    }

    /**
     * Throw InterruptedException if the job has been cancelled.
     * 
     * <p><b>Cancellation Checkpoint:</b> Jobs should call this method periodically
     * during long-running operations. This is the mechanism that allows graceful
     * cancellation - the job explicitly checks if it should stop.</p>
     * 
     * <p><b>Best Practice:</b> Place this at the start of each loop iteration
     * or between major processing steps:</p>
     * <pre>{@code
     * for (Item item : items) {
     *     context.throwIfCancelled(); // Check before processing each item
     *     processItem(item);
     * }
     * }</pre>
     * 
     * <p><b>Why InterruptedException:</b> This exception type signals cancellation
     * and is caught by the Worker, which handles it by marking the job as CANCELLED.</p>
     * 
     * @throws InterruptedException if the job has been cancelled
     */
    public void throwIfCancelled() throws InterruptedException {
        if (cancelled.get()) {
            throw new InterruptedException("Job " + jobId + " was cancelled");
        }
    }

    @Override
    public String toString() {
        return "JobContext{" +
                "jobId='" + jobId + '\'' +
                ", cancelled=" + cancelled.get() +
                '}';
    }


	// ==================== METADATA METHODS ====================
	
    /**
     * Add metadata to the job context.
     * 
     * <p>Metadata can be used to pass state between retries or to collect
     * execution statistics. For example, track items processed, errors encountered,
     * or partial results.</p>
     * 
     * <p><b>Thread Safety:</b> This map uses ConcurrentHashMap for lock-free
     * thread-safe access. Multiple threads can safely add and retrieve metadata
     * concurrently without external synchronization.</p>
     * 
     * @param key the metadata key
     * @param value the metadata value (can be any object)
     */
    public void addMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    /**
     * Retrieve metadata by key.
     * 
     * @param key the metadata key
     * @return the metadata value, or null if not found
     */
    public Object getMetadata(String key) {
        return metadata.get(key);
    }

    /**
     * Get all metadata as a new map.
     * 
     * <p>Returns a defensive copy to prevent external modification.</p>
     * 
     * @return a new HashMap containing all metadata entries
     */
    public Map<String, Object> getAllMetadata() {
        return new HashMap<>(metadata); // Defensive copy
    }
}

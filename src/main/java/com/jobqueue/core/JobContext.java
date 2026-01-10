package com.jobqueue.core;

import com.jobqueue.db.JobRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Context information for job execution.
 * Provides access to job metadata, logging, progress tracking,
 * and cancellation checking during job execution.
 */
public class JobContext {
    private final String jobId;
    private final JobRepository repository;
    private final AtomicBoolean cancelled;
	private final Map<String, Object> metadata;

    /**
     * Create a new job context
     * @param jobId the unique identifier of the job
     * @param repository the job repository for database operations
     */
    public JobContext(String jobId, JobRepository repository) {
        this.jobId = jobId;
        this.repository = repository;
        this.cancelled = new AtomicBoolean(false);
		this.metadata = new HashMap<>();
	}

    /**
     * Get the job ID
     * @return the job's unique identifier
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Log a message to the job_logs table
     * @param level the log level (INFO, WARN, ERROR, DEBUG)
     * @param message the log message
     */
    public void log(String level, String message) {
        String sql = "INSERT INTO job_logs (job_id, timestamp, level, message) VALUES (?, ?, ?, ?)";
        
        try (Connection conn = repository.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, level);
            stmt.setString(4, message);
            
            stmt.executeUpdate();
        } catch (SQLException e) {
            // Log to system logger as fallback
            System.err.println("Failed to log to database: " + e.getMessage());
        }
    }

    /**
     * Update the job's progress status message
     * @param status the status message to update
     */
    public void updateProgress(String status) {
        log("INFO", status);
    }

    /**
     * Check if the job has been cancelled
     * @return true if the job was cancelled, false otherwise
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Mark this job as cancelled
     */
    public void cancel() {
        cancelled.set(true);
    }

    /**
     * Throw InterruptedException if the job has been cancelled.
     * Jobs should call this periodically during long-running operations
     * to allow for graceful cancellation.
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


	// METADATA methods
    public void addMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    public Object getMetadata(String key) {
        return metadata.get(key);
    }

    public Map<String, Object> getAllMetadata() {
        return new HashMap<>(metadata);
    }
}

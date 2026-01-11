package com.jobqueue.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.jobqueue.db.JobRepository;

public class JobContext {
    private final String jobId;
    private final JobRepository repository;
    private final AtomicBoolean cancelled;
	private final Map<String, Object> metadata;

    public JobContext(String jobId, JobRepository repository) {
        this.jobId = jobId;
        this.repository = repository;
        this.cancelled = new AtomicBoolean(false);
		this.metadata = new ConcurrentHashMap<>();
	}

    public String getJobId() {
        return jobId;
    }

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
            System.err.println("Failed to log to database: " + e.getMessage());
        }
    }

    public void updateProgress(String status) {
        log("INFO", status);
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public void cancel() {
        cancelled.set(true);
    }

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
                '}';    }


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

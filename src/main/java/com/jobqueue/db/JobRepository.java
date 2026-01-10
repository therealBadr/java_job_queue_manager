package com.jobqueue.db;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobStatus;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Repository for job persistence and retrieval.
 * All methods use PreparedStatement and try-with-resources for safe resource management.
 */
public class JobRepository {
    private final Database database;

    /**
     * Simple POJO to hold job data retrieved from database
     */
    public static class JobData {
        private String id;
        private String name;
        private String type;
        private String payload;
        private int priority;
        private JobStatus status;
        private LocalDateTime scheduledTime;
        private LocalDateTime createdAt;
        private LocalDateTime startedAt;
        private LocalDateTime completedAt;
        private String errorMessage;
        private int retryCount;
        private int maxRetries;

        // Getters and Setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }

        public int getPriority() { return priority; }
        public void setPriority(int priority) { this.priority = priority; }

        public JobStatus getStatus() { return status; }
        public void setStatus(JobStatus status) { this.status = status; }

        public LocalDateTime getScheduledTime() { return scheduledTime; }
        public void setScheduledTime(LocalDateTime scheduledTime) { this.scheduledTime = scheduledTime; }

        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

        public LocalDateTime getStartedAt() { return startedAt; }
        public void setStartedAt(LocalDateTime startedAt) { this.startedAt = startedAt; }

        public LocalDateTime getCompletedAt() { return completedAt; }
        public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }

        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

        public int getRetryCount() { return retryCount; }
        public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

        @Override
        public String toString() {
            return "JobData{id='" + id + "', type='" + type + "', status=" + status + ", priority=" + priority + "}";
        }
    }

    public JobRepository(Database database) {
        this.database = database;
    }

    /**
     * Get database connection (for use by JobContext)
     * @return database connection
     * @throws SQLException if unable to get connection
     */
    public Connection getConnection() throws SQLException {
        return database.getConnection();
    }

    /**
     * Submit a new job to the database with PENDING status.
     * Generates a UUID for the job ID.
     * 
     * @param job the job to submit
     * @throws SQLException if database operation fails
     */
    public void submitJob(Job job) throws SQLException {
        String sql = "INSERT INTO jobs (id, name, type, payload, priority, status, scheduled_time, created_at, retry_count, max_retries) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            // Use the job's ID instead of generating a new one
            String jobId = job.getId();
            LocalDateTime now = LocalDateTime.now();
            
            stmt.setString(1, jobId);
            stmt.setString(2, job.getType()); // Using type as name for now
            stmt.setString(3, job.getType());
            stmt.setString(4, job.getPayload());
            stmt.setInt(5, job.getPriority());
            stmt.setString(6, JobStatus.PENDING.name());
            stmt.setTimestamp(7, Timestamp.valueOf(now));
            stmt.setTimestamp(8, Timestamp.valueOf(now));
            stmt.setInt(9, 0); // Initial retry count
            stmt.setInt(10, job.getMaxRetries());
            
            stmt.executeUpdate();
            
            System.out.println("Job submitted with ID: " + jobId);
        }
    }

    /**
     * Retrieve a job by its ID.
     * 
     * @param id the job ID
     * @return JobData object if found, null if not found
     * @throws SQLException if database operation fails
     */
    public JobData getJobById(String id) throws SQLException {
        String sql = "SELECT * FROM jobs WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToJob(rs);
                }
            }
        }
        
        return null;
    }

    /**
     * Update job status and related fields.
     * Automatically sets completed_at timestamp for terminal states.
     * 
     * @param jobId the job ID
     * @param status the new status
     * @param errorMessage the error message (can be null)
     * @throws SQLException if database operation fails
     */
    public void updateJobStatus(String jobId, JobStatus status, String errorMessage) throws SQLException {
        String sql = "UPDATE jobs SET status = ?, error_message = ?, completed_at = ? WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, status.name());
            stmt.setString(2, errorMessage);
            
            // Set completed_at for terminal states
            if (status.isTerminal()) {
                stmt.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            } else {
                stmt.setTimestamp(3, null);
            }
            
            stmt.setString(4, jobId);
            
            int rowsUpdated = stmt.executeUpdate();
            
            if (rowsUpdated > 0) {
                System.out.println("Updated job " + jobId + " to status: " + status);
            } else {
                System.out.println("Warning: No job found with ID: " + jobId);
            }
        }
    }

    /**
     * Update job priority.
     * 
     * @param jobId the job ID
     * @param priority the new priority
     * @throws SQLException if database operation fails
     */
    public void updateJobPriority(String jobId, int priority) throws SQLException {
        String sql = "UPDATE jobs SET priority = ? WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, priority);
            stmt.setString(2, jobId);
            
            stmt.executeUpdate();
        }
    }

    /**
     * Update job scheduled time.
     * 
     * @param jobId the job ID
     * @param scheduledTime the new scheduled time
     * @throws SQLException if database operation fails
     */
    public void updateScheduledTime(String jobId, Timestamp scheduledTime) throws SQLException {
        String sql = "UPDATE jobs SET scheduled_time = ? WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, scheduledTime);
            stmt.setString(2, jobId);
            
            stmt.executeUpdate();
        }
    }

    /**
     * Insert a log entry into the job_logs table.
     * 
     * @param jobId the job ID
     * @param level the log level (INFO, WARN, ERROR, DEBUG)
     * @param message the log message
     * @throws SQLException if database operation fails
     */
    public void insertLog(String jobId, String level, String message) throws SQLException {
        String sql = "INSERT INTO job_logs (job_id, timestamp, level, message) VALUES (?, ?, ?, ?)";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, level);
            stmt.setString(4, message);
            
            stmt.executeUpdate();
        }
    }

    /**
     * Get pending jobs ordered by priority (DESC) and scheduled_time (ASC).
     * Higher priority jobs are returned first.
     * 
     * @param limit maximum number of jobs to return
     * @return list of pending jobs
     * @throws SQLException if database operation fails
     */
    public List<JobData> getPendingJobs(int limit) throws SQLException {
        String sql = "SELECT * FROM jobs WHERE status = ? ORDER BY priority DESC, scheduled_time ASC LIMIT ?";
        List<JobData> jobs = new ArrayList<>();
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.PENDING.name());
            stmt.setInt(2, limit);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    jobs.add(mapResultSetToJob(rs));
                }
            }
        }
        
        System.out.println("Retrieved " + jobs.size() + " pending jobs");
        return jobs;
    }

    /**
     * Atomically claim a job for execution.
     * This prevents multiple workers from executing the same job by using
     * a WHERE clause that only matches PENDING jobs.
     * 
     * @param jobId the job ID to claim
     * @return true if job was successfully claimed, false if job was already claimed or doesn't exist
     * @throws SQLException if database operation fails
     */
    public boolean claimJob(String jobId) throws SQLException {
        String sql = "UPDATE jobs SET status = ?, started_at = ? WHERE id = ? AND status = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.RUNNING.name());
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, jobId);
            stmt.setString(4, JobStatus.PENDING.name());
            
            int rowsUpdated = stmt.executeUpdate();
            
            if (rowsUpdated == 1) {
                System.out.println("Successfully claimed job: " + jobId);
                return true;
            } else {
                System.out.println("Failed to claim job: " + jobId + " (already claimed or not found)");
                return false;
            }
        }
    }

    /**
     * Recover jobs that were RUNNING when the system crashed.
     * Resets them back to PENDING status so they can be re-executed.
     * This should be called on system startup.
     * 
     * @return the number of jobs recovered
     * @throws SQLException if database operation fails
     */
    public int recoverCrashedJobs() throws SQLException {
        String sql = "UPDATE jobs SET status = ?, started_at = NULL WHERE status = ?";
        
        Connection conn = null;
        PreparedStatement stmt = null;
        
        try {
            conn = database.getConnection();
            conn.setAutoCommit(false); // Start transaction
            
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, JobStatus.PENDING.name());
            stmt.setString(2, JobStatus.RUNNING.name());
            
            int recoveredCount = stmt.executeUpdate();
            
            conn.commit(); // Commit transaction
            
            System.out.println("Recovered " + recoveredCount + " crashed jobs");
            return recoveredCount;
            
        } catch (SQLException e) {
            // Rollback on error
            if (conn != null) {
                try {
                    conn.rollback();
                    System.err.println("Transaction rolled back due to error: " + e.getMessage());
                } catch (SQLException rollbackEx) {
                    System.err.println("Error during rollback: " + rollbackEx.getMessage());
                }
            }
            throw e;
        } finally {
            // Close resources
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    System.err.println("Error closing statement: " + e.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.setAutoCommit(true); // Restore auto-commit
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Get all jobs from the database.
     * Useful for stream operations and batch processing.
     * 
     * @return list of all jobs
     * @throws SQLException if database operation fails
     */
    public List<JobData> getAllJobs() throws SQLException {
        String sql = "SELECT * FROM jobs ORDER BY created_at DESC";
        List<JobData> jobs = new ArrayList<>();
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                jobs.add(mapResultSetToJob(rs));
            }
        }
        
        System.out.println("Retrieved " + jobs.size() + " total jobs");
        return jobs;
    }

    /**
     * Get completed jobs (SUCCESS or FAILED status).
     * Useful for reporting and analysis.
     * 
     * @return list of completed jobs
     * @throws SQLException if database operation fails
     */
    public List<JobData> getCompletedJobs() throws SQLException {
        String sql = "SELECT * FROM jobs WHERE status = ? OR status = ? ORDER BY completed_at DESC";
        List<JobData> jobs = new ArrayList<>();
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.SUCCESS.name());
            stmt.setString(2, JobStatus.FAILED.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    jobs.add(mapResultSetToJob(rs));
                }
            }
        }
        
        System.out.println("Retrieved " + jobs.size() + " completed jobs");
        return jobs;
    }

    /**
     * Increment the retry count for a job atomically.
     * 
     * <p><b>Thread Safety:</b> Uses SERIALIZABLE transaction isolation to prevent
     * race conditions when multiple threads increment the same job's retry count.
     * This ensures the returned count is accurate and consistent.</p>
     * 
     * @param jobId the job ID
     * @return the new retry count after increment
     * @throws SQLException if database operation fails
     */
    public int incrementRetryCount(String jobId) throws SQLException {
        Connection conn = null;
        try {
            conn = database.getConnection();
            conn.setAutoCommit(false);
            // SERIALIZABLE isolation prevents race conditions
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            
            String updateSql = "UPDATE jobs SET retry_count = retry_count + 1 WHERE id = ?";
            try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
                updateStmt.setString(1, jobId);
                int rows = updateStmt.executeUpdate();
                if (rows == 0) {
                    throw new SQLException("Job not found: " + jobId);
                }
            }
            
            String selectSql = "SELECT retry_count FROM jobs WHERE id = ?";
            int newCount;
            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql)) {
                selectStmt.setString(1, jobId);
                try (ResultSet rs = selectStmt.executeQuery()) {
                    if (rs.next()) {
                        newCount = rs.getInt("retry_count");
                    } else {
                        throw new SQLException("Job not found after update: " + jobId);
                    }
                }
            }
            
            conn.commit();
            System.out.println("Incremented retry count for job " + jobId + " to " + newCount);
            return newCount;
            
        } catch (SQLException e) {
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ignored) {}
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException ignored) {}
            }
        }
    }

    /**
     * Schedule a job for retry after a delay.
     * Sets job back to PENDING status and updates scheduled_time.
     * 
     * @param jobId the job ID
     * @param delayMillis delay in milliseconds before job should be retried
     * @throws SQLException if database operation fails
     */
    public void scheduleRetry(String jobId, long delayMillis) throws SQLException {
        String sql = "UPDATE jobs SET status = ?, scheduled_time = ?, started_at = NULL WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            LocalDateTime scheduledTime = LocalDateTime.now().plusNanos(delayMillis * 1_000_000);
            
            stmt.setString(1, JobStatus.PENDING.name());
            stmt.setTimestamp(2, Timestamp.valueOf(scheduledTime));
            stmt.setString(3, jobId);
            
            int rowsUpdated = stmt.executeUpdate();
            
            if (rowsUpdated > 0) {
                System.out.println("Scheduled retry for job " + jobId + " after " + delayMillis + "ms");
            } else {
                System.out.println("Warning: Could not schedule retry for job " + jobId);
            }
        }
    }

    /**
     * Move a job to the dead letter queue after all retries exhausted.
     * Uses a transaction to ensure atomicity.
     * 
     * @param job the job to move
     * @param finalError the final error message
     * @throws SQLException if database operation fails
     */
    public void moveToDeadLetterQueue(JobData job, String finalError) throws SQLException {
        String insertSql = "INSERT INTO dead_letter_queue " +
                          "(id, name, type, payload, priority, status, scheduled_time, created_at, " +
                          "started_at, completed_at, error_message, retry_count, max_retries, original_error) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String deleteSql = "DELETE FROM jobs WHERE id = ?";
        
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement deleteStmt = null;
        
        try {
            conn = database.getConnection();
            conn.setAutoCommit(false); // Start transaction
            
            // Insert into dead letter queue
            insertStmt = conn.prepareStatement(insertSql);
            insertStmt.setString(1, job.getId());
            insertStmt.setString(2, job.getName());
            insertStmt.setString(3, job.getType());
            insertStmt.setString(4, job.getPayload());
            insertStmt.setInt(5, job.getPriority());
            insertStmt.setString(6, job.getStatus().name());
            insertStmt.setTimestamp(7, job.getScheduledTime() != null ? Timestamp.valueOf(job.getScheduledTime()) : null);
            insertStmt.setTimestamp(8, job.getCreatedAt() != null ? Timestamp.valueOf(job.getCreatedAt()) : null);
            insertStmt.setTimestamp(9, job.getStartedAt() != null ? Timestamp.valueOf(job.getStartedAt()) : null);
            insertStmt.setTimestamp(10, job.getCompletedAt() != null ? Timestamp.valueOf(job.getCompletedAt()) : null);
            insertStmt.setString(11, job.getErrorMessage());
            insertStmt.setInt(12, job.getRetryCount());
            insertStmt.setInt(13, job.getMaxRetries());
            insertStmt.setString(14, finalError);
            insertStmt.executeUpdate();
            
            // Delete from jobs table
            deleteStmt = conn.prepareStatement(deleteSql);
            deleteStmt.setString(1, job.getId());
            deleteStmt.executeUpdate();
            
            conn.commit(); // Commit transaction
            System.out.println("Moved job " + job.getId() + " to dead letter queue");
            
        } catch (SQLException e) {
            // Rollback on error
            if (conn != null) {
                try {
                    conn.rollback();
                    System.err.println("Transaction rolled back: " + e.getMessage());
                } catch (SQLException rollbackEx) {
                    System.err.println("Error during rollback: " + rollbackEx.getMessage());
                }
            }
            throw e;
        } finally {
            // Close resources
            if (insertStmt != null) {
                try { insertStmt.close(); } catch (SQLException e) { /* ignore */ }
            }
            if (deleteStmt != null) {
                try { deleteStmt.close(); } catch (SQLException e) { /* ignore */ }
            }
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Cancel a job if it is PENDING or RUNNING.
     * 
     * @param jobId the job ID to cancel
     * @return true if job was cancelled, false if job was not in a cancellable state
     * @throws SQLException if database operation fails
     */
    public boolean cancelJob(String jobId) throws SQLException {
        String sql = "UPDATE jobs SET status = ?, completed_at = ? WHERE id = ? AND (status = ? OR status = ?)";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.CANCELLED.name());
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, jobId);
            stmt.setString(4, JobStatus.PENDING.name());
            stmt.setString(5, JobStatus.RUNNING.name());
            
            int rowsUpdated = stmt.executeUpdate();
            
            if (rowsUpdated > 0) {
                System.out.println("Cancelled job: " + jobId);
                return true;
            } else {
                System.out.println("Could not cancel job " + jobId + " (not in cancellable state)");
                return false;
            }
        }
    }

    /**
     * Check if a job has been cancelled.
     * 
     * @param jobId the job ID
     * @return true if job status is CANCELLED
     * @throws SQLException if database operation fails
     */
    public boolean isCancelled(String jobId) throws SQLException {
        String sql = "SELECT status FROM jobs WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, jobId);
            
            try (java.sql.ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String status = rs.getString("status");
                    return JobStatus.CANCELLED.name().equals(status);
                }
            }
        }
        
        return false; // Job not found, not cancelled
    }

    /**
     * Get the queue depth (number of PENDING jobs).
     * 
     * @return count of pending jobs
     * @throws SQLException if database operation fails
     */
    public int getQueueDepth() throws SQLException {
        String sql = "SELECT COUNT(*) as count FROM jobs WHERE status = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.PENDING.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int depth = rs.getInt("count");
                    System.out.println("Queue depth: " + depth + " pending jobs");
                    return depth;
                }
            }
        }
        
        return 0;
    }

    /**
     * Get comprehensive metrics about job processing.
     * 
     * @return Map containing metrics: total_processed, success_rate, 
     *         average_execution_time, active_jobs, pending_jobs
     * @throws SQLException if database operation fails
     */
    public java.util.Map<String, Object> getMetrics() throws SQLException {
        java.util.Map<String, Object> metrics = new java.util.HashMap<>();
        
        // Query for counts and execution time
        String sql = "SELECT " +
                    "COUNT(CASE WHEN status IN (?, ?) THEN 1 END) as total_processed, " +
                    "COUNT(CASE WHEN status = ? THEN 1 END) as success_count, " +
                    "COUNT(CASE WHEN status = ? THEN 1 END) as failed_count, " +
                    "COUNT(CASE WHEN status = ? THEN 1 END) as active_jobs, " +
                    "COUNT(CASE WHEN status = ? THEN 1 END) as pending_jobs, " +
                    "AVG(CASE WHEN started_at IS NOT NULL AND completed_at IS NOT NULL " +
                    "    THEN TIMESTAMPDIFF(MILLISECOND, started_at, completed_at) END) as avg_execution_time " +
                    "FROM jobs";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, JobStatus.SUCCESS.name());
            stmt.setString(2, JobStatus.FAILED.name());
            stmt.setString(3, JobStatus.SUCCESS.name());
            stmt.setString(4, JobStatus.FAILED.name());
            stmt.setString(5, JobStatus.RUNNING.name());
            stmt.setString(6, JobStatus.PENDING.name());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long totalProcessed = rs.getLong("total_processed");
                    long successCount = rs.getLong("success_count");
                    long failedCount = rs.getLong("failed_count");
                    long activeJobs = rs.getLong("active_jobs");
                    long pendingJobs = rs.getLong("pending_jobs");
                    Double avgExecutionTime = rs.getDouble("avg_execution_time");
                    
                    metrics.put("total_processed", totalProcessed);
                    metrics.put("active_jobs", activeJobs);
                    metrics.put("pending_jobs", pendingJobs);
                    
                    // Calculate success rate
                    double successRate = 0.0;
                    if (totalProcessed > 0) {
                        successRate = (double) successCount / totalProcessed * 100.0;
                    }
                    metrics.put("success_rate", Math.round(successRate * 100.0) / 100.0);
                    
                    // Average execution time (handle null)
                    if (rs.wasNull()) {
                        metrics.put("average_execution_time", 0.0);
                    } else {
                        metrics.put("average_execution_time", Math.round(avgExecutionTime * 100.0) / 100.0);
                    }
                }
            }
        }
        
        System.out.println("Metrics: " + metrics);
        return metrics;
    }

    /**
     * Move a failed job to the dead letter queue.
     * Removes the job from the jobs table and inserts it into the DLQ with error information.
     * 
     * @param job the job to move to DLQ
     * @param finalError the final error message that caused the job to be moved to DLQ
     * @throws SQLException if database operation fails
     */
    public void moveToDeadLetterQueue(Job job, String finalError) throws SQLException {
        Connection conn = null;
        try {
            conn = database.getConnection();
            conn.setAutoCommit(false); // Begin transaction
            
            // First, fetch the job data from the database
            String selectJob = "SELECT * FROM jobs WHERE id = ?";
            JobData jobData = null;
            
            try (PreparedStatement pstmt = conn.prepareStatement(selectJob)) {
                pstmt.setString(1, job.getId());
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        jobData = mapResultSetToJob(rs);
                    } else {
                        conn.rollback();
                        throw new SQLException("Job not found in database: " + job.getId());
                    }
                }
            }
            
            // Insert into dead_letter_queue
            String insertDLQ = "INSERT INTO dead_letter_queue (id, name, type, payload, priority, " +
                             "status, scheduled_time, created_at, started_at, completed_at, " +
                             "error_message, retry_count, max_retries, original_error, moved_at) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertDLQ)) {
                pstmt.setString(1, jobData.getId());
                pstmt.setString(2, jobData.getName());
                pstmt.setString(3, jobData.getType());
                pstmt.setString(4, jobData.getPayload());
                pstmt.setInt(5, jobData.getPriority());
                pstmt.setString(6, jobData.getStatus().name());
                pstmt.setTimestamp(7, jobData.getScheduledTime() != null ? Timestamp.valueOf(jobData.getScheduledTime()) : null);
                pstmt.setTimestamp(8, jobData.getCreatedAt() != null ? Timestamp.valueOf(jobData.getCreatedAt()) : null);
                pstmt.setTimestamp(9, jobData.getStartedAt() != null ? Timestamp.valueOf(jobData.getStartedAt()) : null);
                pstmt.setTimestamp(10, jobData.getCompletedAt() != null ? Timestamp.valueOf(jobData.getCompletedAt()) : null);
                pstmt.setString(11, jobData.getErrorMessage());
                pstmt.setInt(12, jobData.getRetryCount());
                pstmt.setInt(13, jobData.getMaxRetries());
                pstmt.setString(14, finalError);
                pstmt.setTimestamp(15, Timestamp.valueOf(LocalDateTime.now()));
                
                pstmt.executeUpdate();
            }
            
            // Delete from jobs table
            String deleteJob = "DELETE FROM jobs WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(deleteJob)) {
                pstmt.setString(1, job.getId());
                pstmt.executeUpdate();
            }
            
            conn.commit(); // Commit transaction
            System.out.println("Moved job " + job.getId() + " to DLQ");
            
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback(); // Rollback on error
                    System.err.println("Rolled back transaction for moving job " + job.getId() + " to DLQ");
                } catch (SQLException ex) {
                    System.err.println("Error rolling back transaction: " + ex.getMessage());
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true); // Restore auto-commit
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Get jobs from the dead letter queue.
     * Returns most recently moved jobs first.
     * 
     * @param limit maximum number of jobs to return
     * @return list of jobs in the DLQ
     * @throws SQLException if database operation fails
     */
    public List<JobData> getDLQJobs(int limit) throws SQLException {
        List<JobData> jobs = new ArrayList<>();
        String sql = "SELECT * FROM dead_letter_queue ORDER BY moved_at DESC LIMIT ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setInt(1, limit);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    jobs.add(mapResultSetToJob(rs));
                }
            }
        }
        
        return jobs;
    }

    /**
     * Replay a job from the dead letter queue back to the jobs table.
     * Resets the job to PENDING status with cleared retry count and error information.
     * 
     * @param jobId the ID of the job to replay
     * @return true if job was successfully replayed, false if job not found in DLQ
     * @throws SQLException if database operation fails
     */
    public boolean replayFromDLQ(String jobId) throws SQLException {
        Connection conn = null;
        try {
            conn = database.getConnection();
            conn.setAutoCommit(false); // Begin transaction
            
            // First, check if job exists in DLQ and get its data
            String selectDLQ = "SELECT * FROM dead_letter_queue WHERE id = ?";
            JobData jobData = null;
            
            try (PreparedStatement pstmt = conn.prepareStatement(selectDLQ)) {
                pstmt.setString(1, jobId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        jobData = mapResultSetToJob(rs);
                    } else {
                        conn.rollback();
                        return false; // Job not found in DLQ
                    }
                }
            }
            
            // Insert back into jobs table with reset status
            String insertJob = "INSERT INTO jobs (id, name, type, payload, priority, status, " +
                             "scheduled_time, created_at, retry_count, max_retries) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertJob)) {
                pstmt.setString(1, jobData.getId());
                pstmt.setString(2, jobData.getName());
                pstmt.setString(3, jobData.getType());
                pstmt.setString(4, jobData.getPayload());
                pstmt.setInt(5, jobData.getPriority());
                pstmt.setString(6, JobStatus.PENDING.name());
                pstmt.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now())); // Schedule now
                pstmt.setTimestamp(8, jobData.getCreatedAt() != null ? Timestamp.valueOf(jobData.getCreatedAt()) : Timestamp.valueOf(LocalDateTime.now()));
                pstmt.setInt(9, 0); // Reset retry count
                pstmt.setInt(10, jobData.getMaxRetries());
                
                pstmt.executeUpdate();
            }
            
            // Delete from dead_letter_queue
            String deleteDLQ = "DELETE FROM dead_letter_queue WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(deleteDLQ)) {
                pstmt.setString(1, jobId);
                pstmt.executeUpdate();
            }
            
            conn.commit(); // Commit transaction
            System.out.println("Replayed job " + jobId + " from DLQ");
            return true;
            
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback(); // Rollback on error
                    System.err.println("Rolled back transaction for replaying job " + jobId + " from DLQ");
                } catch (SQLException ex) {
                    System.err.println("Error rolling back transaction: " + ex.getMessage());
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true); // Restore auto-commit
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Clear all jobs from the dead letter queue.
     * 
     * @return the number of jobs deleted from the DLQ
     * @throws SQLException if database operation fails
     */
    public int clearDLQ() throws SQLException {
        String sql = "DELETE FROM dead_letter_queue";
        
        try (Connection conn = database.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            int deleted = pstmt.executeUpdate();
            System.out.println("Cleared " + deleted + " jobs from DLQ");
            return deleted;
        }
    }

    /**
     * Map ResultSet to JobData object.
     * Handles null values appropriately.
     * 
     * @param rs the ResultSet positioned at a row
     * @return JobData object with data from the current row
     * @throws SQLException if unable to read from ResultSet
     */
    private JobData mapResultSetToJob(ResultSet rs) throws SQLException {
        JobData job = new JobData();
        
        job.setId(rs.getString("id"));
        job.setName(rs.getString("name"));
        job.setType(rs.getString("type"));
        job.setPayload(rs.getString("payload"));
        job.setPriority(rs.getInt("priority"));
        job.setStatus(JobStatus.valueOf(rs.getString("status")));
        job.setRetryCount(rs.getInt("retry_count"));
        job.setMaxRetries(rs.getInt("max_retries"));
        job.setErrorMessage(rs.getString("error_message"));
        
        // Handle nullable timestamps
        Timestamp scheduledTime = rs.getTimestamp("scheduled_time");
        if (scheduledTime != null) {
            job.setScheduledTime(scheduledTime.toLocalDateTime());
        }
        
        Timestamp createdAt = rs.getTimestamp("created_at");
        if (createdAt != null) {
            job.setCreatedAt(createdAt.toLocalDateTime());
        }
        
        Timestamp startedAt = rs.getTimestamp("started_at");
        if (startedAt != null) {
            job.setStartedAt(startedAt.toLocalDateTime());
        }
        
        Timestamp completedAt = rs.getTimestamp("completed_at");
        if (completedAt != null) {
            job.setCompletedAt(completedAt.toLocalDateTime());
        }
        
        return job;
    }
}

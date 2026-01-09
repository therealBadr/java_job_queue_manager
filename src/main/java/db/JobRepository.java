package db;

import core.Job;
import core.JobStatus;
import java.util.ArrayList;
import java.util.List;

/**
 * Repository for job database operations.
 * 
 * This is a STUB implementation to allow the code to compile while
 * Student B works on the full database layer. Replace methods with
 * actual JDBC implementations later.
 * 
 * Responsibilities:
 * - CRUD operations on jobs table
 * - Atomic job claiming for concurrent access
 * - Job status updates
 * - Logging to job_logs table
 * - Querying jobs by status and priority
 */
public class JobRepository {
    
    /**
     * Creates a new JobRepository.
     * 
     * @param database the database connection manager (Student B's implementation)
     */
    public JobRepository(Database database) {
        System.out.println("[STUB] JobRepository created with database connection");
        // TODO: Store database reference for JDBC operations
    }
    
    /**
     * Retrieves pending jobs ordered by priority (DESC) and scheduled time (ASC).
     * 
     * SQL Query (for reference):
     * SELECT * FROM jobs 
     * WHERE status = 'PENDING' 
     * ORDER BY priority DESC, scheduled_time ASC 
     * LIMIT ?
     * 
     * @param limit maximum number of jobs to return
     * @return list of pending jobs (empty list for now - stub)
     */
    public List<Job> getPendingJobs(int limit) {
        System.out.println("[STUB] getPendingJobs called with limit: " + limit);
        // TODO: Implement actual database query
        return new ArrayList<>();
    }
    
    /**
     * Atomically claims a job by setting its status to RUNNING.
     * 
     * This is a critical method for thread safety. Multiple schedulers
     * may try to claim the same job, but only ONE should succeed.
     * 
     * Implementation Strategy:
     * UPDATE jobs 
     * SET status = 'RUNNING', started_at = ? 
     * WHERE id = ? AND status = 'PENDING'
     * 
     * Check if rowsAffected == 1 to confirm successful claim.
     * 
     * @param jobId the ID of the job to claim
     * @return true if job was successfully claimed, false if already claimed
     */
    public boolean claimJob(String jobId) {
        System.out.println("[STUB] claimJob called for job: " + jobId);
        // TODO: Implement atomic UPDATE with WHERE clause
        return false; // Return false for now (no jobs available)
    }
    
    /**
     * Updates the status of a job and optionally sets an error message.
     * 
     * SQL Query (for reference):
     * UPDATE jobs 
     * SET status = ?, completed_at = ?, error_message = ? 
     * WHERE id = ?
     * 
     * @param jobId the ID of the job to update
     * @param status the new status
     * @param errorMessage error message (null if no error)
     */
    public void updateJobStatus(String jobId, JobStatus status, String errorMessage) {
        System.out.println("[STUB] updateJobStatus: Job " + jobId + " -> " + status + 
                          (errorMessage != null ? " (Error: " + errorMessage + ")" : ""));
        // TODO: Implement actual UPDATE query
    }
    
    /**
     * Retrieves all jobs with a specific status.
     * 
     * Used primarily for crash recovery to find RUNNING jobs.
     * 
     * SQL Query (for reference):
     * SELECT * FROM jobs WHERE status = ?
     * 
     * @param status the status to filter by
     * @return list of jobs with the given status (empty list for now - stub)
     */
    public List<Job> getJobsByStatus(JobStatus status) {
        System.out.println("[STUB] getJobsByStatus called for status: " + status);
        // TODO: Implement actual database query
        return new ArrayList<>();
    }
    
    /**
     * Logs a message for a specific job to the job_logs table.
     * 
     * SQL Query (for reference):
     * INSERT INTO job_logs (job_id, log_level, message, created_at) 
     * VALUES (?, ?, ?, ?)
     * 
     * @param jobId the ID of the job
     * @param level log level (INFO, WARN, ERROR, DEBUG)
     * @param message the log message
     */
    public void logJobMessage(String jobId, String level, String message) {
        System.out.println("[STUB] logJobMessage: [" + level + "] Job " + jobId + ": " + message);
        // TODO: Implement INSERT into job_logs table
    }
    
    /**
     * Submits a new job to the queue.
     * 
     * SQL Query (for reference):
     * INSERT INTO jobs (id, type, payload, priority, status, scheduled_time, created_at) 
     * VALUES (?, ?, ?, ?, 'PENDING', ?, ?)
     * 
     * @param job the job to submit
     */
    public void submitJob(Job job) {
        System.out.println("[STUB] submitJob: " + job.getId() + " (type: " + job.getType() + ")");
        // TODO: Implement INSERT into jobs table
    }
}

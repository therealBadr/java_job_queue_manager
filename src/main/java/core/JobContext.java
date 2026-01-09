package core;

import db.JobRepository;

/**
 * Provides execution context and helper methods to jobs during execution.
 * 
 * JobContext acts as a bridge between the executing job and the system's
 * infrastructure (database, logging, etc.). It's passed to every job's
 * execute() method.
 * 
 * Design Benefits:
 * - Jobs don't need direct access to the repository
 * - Logging is centralized and consistent
 * - Easy to add new context features without changing Job interface
 * - Can be mocked for testing
 */
public class JobContext {
    
    /** The ID of the job currently being executed */
    private final String jobId;
    
    /** 
     * Reference to the repository for logging and status updates.
     * NOTE: Student B will implement the actual logging method in JobRepository.
     * For now, we'll use console output as a placeholder.
     */
    private final JobRepository repository;

    public JobContext(String jobId, JobRepository repository) {
        this.jobId = jobId;
        this.repository = repository;
    }
    public void log(String level, String message) {
        // TODO: Student B will implement repository.logJobMessage(jobId, level, message)
        // For now, print to console as a placeholder
        String timestamp = java.time.LocalDateTime.now().toString();
        System.out.println("[" + timestamp + "] [" + level + "] Job " + jobId + ": " + message);
    }
    
    public void updateProgress(String status) {
        // For now, just log it
        // In bonus section, you could update a progress field in the database
        System.out.println("Job " + jobId + " progress: " + status);
        
        // Alternatively, log it properly:
        // log("PROGRESS", status);
    }
    

    public String getJobId() {
        return jobId;
    }
    

    public JobRepository getRepository() {
        return repository;
    }
}

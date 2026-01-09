package core;

/**
 * Core interface representing an executable job in the queue system.
 * 
 * All concrete job types (EmailJob, CleanupJob, ReportJob) must implement
 * this interface. The interface enables polymorphic behavior - the scheduler
 * and workers can handle any job type without knowing the specific implementation.
 * 
 * Design Pattern: This follows the Command Pattern, where each job encapsulates
 * an action to be executed.
 */
public interface Job {

    String getId();

    String getType();

    void execute(JobContext context) throws Exception;

    String getPayload();

    int getPriority();

    JobStatus getStatus();
}


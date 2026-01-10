package com.jobqueue.jobs;

import com.jobqueue.core.BaseJob;
import com.jobqueue.core.JobContext;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * A job that fails by design to test DLQ and retry mechanisms.
 * Jobs can be "fixed" by adding their IDs to the fixedJobs set.
 */
public class FailingJob extends BaseJob {
    private static final Logger logger = Logger.getLogger(FailingJob.class.getName());
    
    // Static set of job IDs that have been "fixed" and should now succeed
    private static final Set<String> fixedJobs = ConcurrentHashMap.newKeySet();
    
    public static class FailureData {
        public String taskName;
        public String reason;
        
        public FailureData() {}
        
        public FailureData(String taskName, String reason) {
            this.taskName = taskName;
            this.reason = reason;
        }
    }
    
    public FailingJob() {
        super();
        setMaxRetries(2); // Only retry twice before DLQ
    }
    
    /**
     * Constructor for scheduler instantiation
     */
    public FailingJob(String id, String payload, int priority) {
        super(id, payload, priority);
        setMaxRetries(2);
    }
    
    public void setFailureData(String taskName, String reason) {
        FailureData data = new FailureData(taskName, reason);
        setPayload(toPayload(data));
    }
    
    /**
     * Mark a job ID as "fixed" so it will succeed on next execution
     */
    public static void fixJob(String jobId) {
        fixedJobs.add(jobId);
        logger.info("Job marked as fixed: " + jobId);
    }
    
    /**
     * Check if a job has been fixed
     */
    public static boolean isFixed(String jobId) {
        return fixedJobs.contains(jobId);
    }
    
    /**
     * Clear all fixed jobs
     */
    public static void clearFixedJobs() {
        fixedJobs.clear();
        logger.info("Cleared all fixed jobs");
    }
    
    /**
     * Get count of fixed jobs
     */
    public static int getFixedJobCount() {
        return fixedJobs.size();
    }
    
    @Override
    public void execute(JobContext context) throws Exception {
        logger.info("Executing FailingJob: " + getId());
        
        // Parse failure data from payload
        FailureData data = fromPayload(getPayload(), FailureData.class);
        
        if (data == null || data.taskName == null) {
            throw new IllegalArgumentException("Invalid failure data");
        }
        
        context.log("INFO", "Processing task: " + data.taskName);
        logger.info("Task: " + data.taskName + ", Reason: " + data.reason);
        
        // Simulate some work
        context.updateProgress("Starting task...");
        Thread.sleep(1000);
        
        // Check if this job has been "fixed"
        if (isFixed(getId())) {
            context.log("INFO", "Job has been fixed - executing successfully!");
            logger.info("Job " + getId() + " has been fixed - executing successfully!");
            
            context.updateProgress("Processing fixed task...");
            Thread.sleep(2000);
            
            context.log("INFO", "Task completed successfully after fix");
            logger.info("Task completed successfully: " + data.taskName);
            
            context.addMetadata("taskName", data.taskName);
            context.addMetadata("status", "fixed_and_succeeded");
            context.addMetadata("fixApplied", true);
            
            return; // Success!
        }
        
        // Otherwise, fail as designed
        context.log("ERROR", "Simulated failure: " + data.reason);
        logger.warning("Job " + getId() + " is failing: " + data.reason);
        
        // Throw exception to trigger retry/DLQ
        throw new RuntimeException("Simulated failure: " + data.reason + " (Task: " + data.taskName + ")");
    }
    
    @Override
    public int getPriority() {
        return 5; // Normal priority
    }
    
    @Override
    public boolean isCancellable() {
        return true;
    }
}

package com.jobqueue.engine;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.JobRepository;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker class that executes a single job
 */
public class Worker implements Runnable {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    
    private final Job job;
    private final JobRepository repository;

    /**
     * Create a new Worker to execute a job
     * @param job the job to execute
     * @param repository the repository for job state management
     */
    public Worker(Job job, JobRepository repository) {
        this.job = job;
        this.repository = repository;
    }

    @Override
    public void run() {
        String jobId = job.getId();
        logger.info("Worker starting execution of job: " + jobId + " (type: " + job.getType() + ")");
        
        // Create job context for execution
        JobContext context = new JobContext(jobId, repository);
        
        try {
            // Execute the job
            logger.info("Executing job: " + jobId);
            context.log("INFO", "Job execution started");
            
            job.execute(context);
            
            // Job executed successfully
            logger.info("Job completed successfully: " + jobId);
            context.log("INFO", "Job execution completed successfully");
            
            // Update job status to SUCCESS
            repository.updateJobStatus(jobId, JobStatus.SUCCESS, null);
            
        } catch (InterruptedException e) {
            // Job was cancelled
            logger.warning("Job was cancelled: " + jobId);
            context.log("WARN", "Job was cancelled by user or system");
            
            try {
                repository.updateJobStatus(jobId, JobStatus.CANCELLED, "Job was cancelled");
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Failed to update cancelled job status: " + jobId, ex);
            }
            
            // Restore interrupt status
            Thread.currentThread().interrupt();
            
        } catch (Exception e) {
            // Job execution failed
            logger.log(Level.SEVERE, "Job execution failed: " + jobId, e);
            String errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            context.log("ERROR", "Job execution failed: " + errorMessage);
            
            try {
                // Get current retry count from repository
                JobRepository.JobData jobData = repository.getJobById(jobId);
                if (jobData == null) {
                    logger.severe("Cannot find job in database: " + jobId);
                    return;
                }
                
                int retryCount = jobData.getRetryCount();
                int maxRetries = job.getMaxRetries();
                
                // Check if retries remaining
                if (retryCount < maxRetries) {
                    // Increment retry count
                    int newRetryCount = repository.incrementRetryCount(jobId);
                    
                    // Calculate exponential backoff delay: 2^retryCount minutes in milliseconds
                    long delayMillis = (long) Math.pow(2, newRetryCount) * 60 * 1000;
                    long delayMinutes = (long) Math.pow(2, newRetryCount);
                    
                    // Schedule the retry
                    repository.scheduleRetry(jobId, delayMillis);
                    
                    // Log retry schedule
                    logger.info("Job " + jobId + " will retry in " + delayMinutes + " minutes (attempt " + 
                               newRetryCount + "/" + maxRetries + ")");
                    context.log("INFO", "Retry scheduled in " + delayMinutes + " minutes (attempt " + 
                               newRetryCount + "/" + maxRetries + ")");
                    
                } else {
                    // No retries remaining
                    // Update status to FAILED
                    repository.updateJobStatus(jobId, JobStatus.FAILED, errorMessage);
                    
                    // Move to dead letter queue
                    jobData = repository.getJobById(jobId); // Refresh to get updated data
                    if (jobData != null) {
                        repository.moveToDeadLetterQueue(jobData, errorMessage);
                        logger.info("Job " + jobId + " exhausted retries, moved to DLQ");
                        context.log("ERROR", "Job exhausted retries after " + retryCount + " attempts, moved to DLQ");
                    }
                }
                
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Failed to handle job failure for: " + jobId, ex);
                context.log("ERROR", "Failed to handle job failure: " + ex.getMessage());
            }
        }
        
        logger.info("Worker finished processing job: " + jobId);
    }
}

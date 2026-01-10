package com.jobqueue.engine;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.JobRepository;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker class that executes a single job in the thread pool.
 * 
 * <p>Each Worker is a Runnable that handles the complete lifecycle of a single job:</p>
 * <ul>
 *   <li>Create execution context (JobContext)</li>
 *   <li>Execute the job's business logic</li>
 *   <li>Handle success: update status to SUCCESS</li>
 *   <li>Handle failure: implement exponential backoff retry</li>
 *   <li>Handle cancellation: update status to CANCELLED</li>
 *   <li>Move to Dead Letter Queue after retry exhaustion</li>
 * </ul>
 * 
 * <p><b>Thread Safety:</b> Each Worker runs in its own thread from the executor pool.
 * Workers do not share state - each has its own Job instance and JobContext.</p>
 * 
 * <p><b>Retry Mechanism:</b></p>
 * <ul>
 *   <li>Exponential backoff: delay = 2^retry_count minutes</li>
 *   <li>Retry 1: 2 minutes (2^1)</li>
 *   <li>Retry 2: 4 minutes (2^2)</li>
 *   <li>Retry 3: 8 minutes (2^3)</li>
 *   <li>After max retries: move to Dead Letter Queue</li>
 * </ul>
 * 
 * <p><b>Error Handling Strategy:</b></p>
 * <ul>
 *   <li>InterruptedException → CANCELLED (graceful cancellation)</li>
 *   <li>Any other Exception → schedule retry with exponential backoff</li>
 *   <li>Retry exhausted → FAILED + move to DLQ</li>
 * </ul>
 * 
 * <p><b>Design Decision:</b> Why exponential backoff? Transient failures (network glitches,
 * temporary service outages) often resolve within minutes. Exponential delays give
 * the system time to recover while avoiding retry storms that could worsen the problem.</p>
 * 
 * @author Job Queue Team
 * @see Scheduler
 * @see JobContext
 * @see JobRepository#scheduleRetry(String, long)
 */
public class Worker implements Runnable {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    
    private final Job job;                  // The job to execute
    private final JobRepository repository; // Database access for status updates

    /**
     * Create a new Worker to execute a job.
     * 
     * <p>The Worker is created by the Scheduler when a job is claimed from the database.
     * It is immediately submitted to the executor thread pool.</p>
     * 
     * @param job the job to execute (already claimed from database)
     * @param repository the repository for job state management and logging
     */
    public Worker(Job job, JobRepository repository) {
        this.job = job;
        this.repository = repository;
    }

    /**
     * Execute the job and handle all outcomes.
     * 
     * <p><b>Execution Flow:</b></p>
     * <ol>
     *   <li>Create JobContext with cancellation support</li>
     *   <li>Call job.execute(context)</li>
     *   <li>On success: update to SUCCESS</li>
     *   <li>On cancellation: update to CANCELLED</li>
     *   <li>On failure: implement retry logic with exponential backoff</li>
     * </ol>
     * 
     * <p><b>Thread Safety:</b> This method runs in a worker thread from the pool.
     * Multiple workers can execute simultaneously, each with their own job.</p>
     */
    @Override
    public void run() {
        String jobId = job.getId();
        logger.info("Worker starting execution of job: " + jobId + " (type: " + job.getType() + ")");
        
        // Create job context for execution
        // The context provides logging, cancellation checking, and metadata storage
        JobContext context = new JobContext(jobId, repository);
        
        try {
            // === EXECUTE JOB ===
            logger.info("Executing job: " + jobId);
            context.log("INFO", "Job execution started");
            
            // Call the job's business logic
            // This is where the actual work happens (send email, generate report, etc.)
            job.execute(context);
            
            // === SUCCESS PATH ===
            logger.info("Job completed successfully: " + jobId);
            context.log("INFO", "Job execution completed successfully");
            
            // Update job status to SUCCESS in database
            repository.updateJobStatus(jobId, JobStatus.SUCCESS, null);
            
        } catch (InterruptedException e) {
            // === CANCELLATION PATH ===
            // Job was cancelled by calling context.cancel() or context.throwIfCancelled()
            logger.warning("Job was cancelled: " + jobId);
            context.log("WARN", "Job was cancelled by user or system");
            
            try {
                repository.updateJobStatus(jobId, JobStatus.CANCELLED, "Job was cancelled");
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Failed to update cancelled job status: " + jobId, ex);
            }
            
            // Restore interrupt status for proper thread pool shutdown
            Thread.currentThread().interrupt();
            
        } catch (Exception e) {
            // === FAILURE PATH ===
            // Job execution failed - implement retry logic
            logger.log(Level.SEVERE, "Job execution failed: " + jobId, e);
            String errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            context.log("ERROR", "Job execution failed: " + errorMessage);
            
            try {
                // Get current job state from database
                JobRepository.JobData jobData = repository.getJobById(jobId);
                if (jobData == null) {
                    logger.severe("Cannot find job in database: " + jobId);
                    return;
                }
                
                int retryCount = jobData.getRetryCount();
                int maxRetries = job.getMaxRetries();
                
                // === RETRY LOGIC ===
                if (retryCount < maxRetries) {
                    // Still have retries remaining
                    
                    // Increment retry count atomically in database
                    int newRetryCount = repository.incrementRetryCount(jobId);
                    
                    // EXPONENTIAL BACKOFF CALCULATION
                    // Formula: delay = 2^retry_count minutes
                    // Why: Gives transient failures time to resolve
                    // Example: 1st retry = 2 min, 2nd = 4 min, 3rd = 8 min
                    long delayMillis = (long) Math.pow(2, newRetryCount) * 60 * 1000;
                    long delayMinutes = (long) Math.pow(2, newRetryCount);
                    
                    // Schedule the retry by updating scheduled_time in database
                    // The scheduler will pick it up when the time comes
                    repository.scheduleRetry(jobId, delayMillis);
                    
                    // Log retry schedule for visibility
                    logger.info("Job " + jobId + " will retry in " + delayMinutes + " minutes (attempt " + 
                               newRetryCount + "/" + maxRetries + ")");
                    context.log("INFO", "Retry scheduled in " + delayMinutes + " minutes (attempt " + 
                               newRetryCount + "/" + maxRetries + ")");
                    
                } else {
                    // === RETRY EXHAUSTION ===
                    // No retries remaining - permanent failure
                    
                    // Update status to FAILED
                    repository.updateJobStatus(jobId, JobStatus.FAILED, errorMessage);
                    
                    // MOVE TO DEAD LETTER QUEUE
                    // Why: Preserve failed jobs for analysis and potential replay
                    // The DLQ stores original error, retry count, and all job data
                    jobData = repository.getJobById(jobId); // Refresh to get updated data
                    if (jobData != null) {
                        repository.moveToDeadLetterQueue(jobData, errorMessage);
                        logger.info("Job " + jobId + " exhausted retries, moved to DLQ");
                        context.log("ERROR", "Job exhausted retries after " + retryCount + " attempts, moved to DLQ");
                    }
                }
                
            } catch (Exception ex) {
                // Failure to handle failure - this is critical
                // Log extensively but don't throw - we want worker to complete
                logger.log(Level.SEVERE, "Failed to handle job failure for: " + jobId, ex);
                context.log("ERROR", "Failed to handle job failure: " + ex.getMessage());
            }
        }
        
        logger.info("Worker finished processing job: " + jobId);
    }
}

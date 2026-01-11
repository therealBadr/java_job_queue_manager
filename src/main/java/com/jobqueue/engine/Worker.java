package com.jobqueue.engine;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.JobRepository;

public class Worker implements Runnable {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    
    private final Job job;
    private final JobRepository repository;

    public Worker(Job job, JobRepository repository) {
        this.job = job;
        this.repository = repository;
    }

    @Override
    public void run() {
        String jobId = job.getId();
        String threadName = Thread.currentThread().getName();
        logger.info("Worker thread [" + threadName + "] starting execution of job: " + jobId + " (type: " + job.getType() + ")");
        
        JobContext context = null;
        
        try {
            context = new JobContext(jobId, repository);
            
            logger.info("Executing job: " + jobId);
            context.log("INFO", "Job execution started in thread: " + threadName);
            
            job.execute(context);
            
            logger.info("Job completed successfully: " + jobId);
            context.log("INFO", "Job execution completed successfully");
            
            repository.updateJobStatus(jobId, JobStatus.SUCCESS, null);
            
        } catch (InterruptedException e) {
            logger.warning("Job was cancelled: " + jobId);
            if (context != null) {
                context.log("WARN", "Job was cancelled by user or system");
            }
            
            try {
                repository.updateJobStatus(jobId, JobStatus.CANCELLED, "Job was cancelled");
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Failed to update cancelled job status: " + jobId, ex);
            }
            
            Thread.currentThread().interrupt();
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Job execution failed: " + jobId, e);
            String errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            
            if (context != null) {
                context.log("ERROR", "Job execution failed: " + errorMessage);
            }
            
            try {
                JobRepository.JobData jobData = repository.getJobById(jobId);
                if (jobData == null) {
                    logger.severe("Cannot find job in database: " + jobId);
                    return;
                }
                
                int retryCount = jobData.getRetryCount();
                int maxRetries = job.getMaxRetries();
                
                if (retryCount < maxRetries) {
                    int newRetryCount = repository.incrementRetryCount(jobId);
                    
                    long delayMillis = (long) Math.pow(2, newRetryCount) * 60 * 1000;
                    long delayMinutes = (long) Math.pow(2, newRetryCount);
                    
                    repository.scheduleRetry(jobId, delayMillis);
                    
                    logger.info("Job " + jobId + " will retry in " + delayMinutes + " minutes (attempt " + 
                               newRetryCount + "/" + maxRetries + ")");
                    
                    if (context != null) {
                        context.log("INFO", "Retry scheduled in " + delayMinutes + " minutes (attempt " + 
                                   newRetryCount + "/" + maxRetries + ")");
                    }
                    
                } else {
                    repository.updateJobStatus(jobId, JobStatus.FAILED, errorMessage);
                    
                    jobData = repository.getJobById(jobId);
                    if (jobData != null) {
                        repository.moveToDeadLetterQueue(jobData, errorMessage);
                        logger.info("Job " + jobId + " exhausted retries, moved to DLQ");
                        
                        if (context != null) {
                            context.log("ERROR", "Job exhausted retries after " + retryCount + " attempts, moved to DLQ");
                        }
                    }
                }
                
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Failed to handle job failure for: " + jobId, ex);
                if (context != null) {
                    context.log("ERROR", "Failed to handle job failure: " + ex.getMessage());
                }
            }
            
        } finally {
            logger.info("Worker thread [" + threadName + "] finished processing job: " + jobId + " - CLEANUP complete");
            
            if (context != null) {
                try {
                    context.log("INFO", "Worker thread cleanup: resources released");
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Failed to log cleanup for job: " + jobId, e);
                }
            }
            
            logger.fine("Thread [" + threadName + "] terminating");
        }
    }
}

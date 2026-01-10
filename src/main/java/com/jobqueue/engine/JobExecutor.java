package com.jobqueue.engine;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;

import java.util.logging.Logger;

/**
 * Executes individual jobs
 */
public class JobExecutor {
    private static final Logger logger = Logger.getLogger(JobExecutor.class.getName());

    /**
     * Execute a job with the given context
     * @param job The job to execute
     * @param context The execution context
     * @throws Exception if job execution fails
     */
    public void execute(Job job, JobContext context) throws Exception {
        logger.info("Executing job: " + job.getId() + " of type: " + job.getType());
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Call the job's execute method
            job.execute(context);
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Job " + job.getId() + " completed in " + duration + "ms");
            
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            logger.severe("Job " + job.getId() + " failed after " + duration + "ms: " + e.getMessage());
            throw e;
        }
    }
}

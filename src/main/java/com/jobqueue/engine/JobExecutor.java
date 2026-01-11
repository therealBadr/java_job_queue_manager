package com.jobqueue.engine;

import java.util.logging.Logger;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;

public class JobExecutor {
    private static final Logger logger = Logger.getLogger(JobExecutor.class.getName());

    public void execute(Job job, JobContext context) throws Exception {
        logger.info("Executing job: " + job.getId() + " of type: " + job.getType());
        
        long startTime = System.currentTimeMillis();
        
        try {
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

package com.jobqueue.jobs;

import java.util.Random;
import java.util.logging.Logger;

import com.jobqueue.core.BaseJob;
import com.jobqueue.core.JobContext;

// Job for cleaning up old files and data
public class CleanupJob extends BaseJob {
    private static final Logger logger = Logger.getLogger(CleanupJob.class.getName());

    public static class CleanupData {
        public String directory;
        public int daysOld;
        public String filePattern;

        public CleanupData() {}

        public CleanupData(String directory, int daysOld, String filePattern) {
            this.directory = directory;
            this.daysOld = daysOld;
            this.filePattern = filePattern;
        }
    }

    public CleanupJob() {
        super();
        setMaxRetries(2);
    }

    public CleanupJob(String id, String payload, int priority) {
        super(id, payload, priority);
        setMaxRetries(2);
    }

    public void setCleanupData(String directory, int daysOld, String filePattern) {
        CleanupData data = new CleanupData(directory, daysOld, filePattern);
        setPayload(toPayload(data));
    }

    @Override
    public void execute(JobContext context) throws Exception {
        logger.info("Executing CleanupJob: " + getId());
        
        // Parse cleanup data from payload
        CleanupData data = fromPayload(getPayload(), CleanupData.class);
        
        if (data == null || data.directory == null) {
            throw new IllegalArgumentException("Invalid cleanup data");
        }

        // Log start of cleanup
        context.log("INFO", "Starting file cleanup in " + data.directory);
        logger.info("Cleaning up directory: " + data.directory);
        logger.info("Removing files older than " + data.daysOld + " days");
        
        // Simulate directory scanning
        Thread.sleep(1500);
        
        // Check for cancellation
        context.throwIfCancelled();
        
        // Generate random number of files found (20-100)
        Random random = new Random();
        int filesFound = 20 + random.nextInt(81); // 20 to 100
        
        // Log files found
        context.log("INFO", "Found " + filesFound + " files to clean");
        logger.info("Found " + filesFound + " files to clean");
        
        // Simulate file deletion
        for (int i = 1; i <= filesFound; i++) {
            // Check for cancellation every 10 iterations
            if (i % 10 == 0) {
                context.throwIfCancelled();
            }
            
            // Update progress
            context.updateProgress("Cleaned " + i + "/" + filesFound + " files");
            
            // Simulate file deletion
            Thread.sleep(50);
        }
        
        // Log completion
        context.log("INFO", "Cleanup completed: " + filesFound + " files removed");
        logger.info("Cleanup completed. Files deleted: " + filesFound);
        
        // Add metadata to context
        context.addMetadata("directory", data.directory);
        context.addMetadata("filesDeleted", filesFound);
    }

    @Override
    public int getPriority() {
        return 3;
    }
}

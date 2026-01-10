package com.jobqueue.app;

import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.ReportJob;

/**
 * Demonstration of job cancellation functionality.
 * Shows how to cancel jobs in different states (PENDING and RUNNING).
 */
public class JobCancellationDemo {
    
    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║       JOB CANCELLATION DEMONSTRATION                 ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\n");
        
        Database database = new Database();
        
        try {
            // Initialize database
            database.initialize();
            System.out.println("✓ Database initialized\n");
            
            JobRepository repository = new JobRepository(database);
            Scheduler scheduler = new Scheduler(2, repository);
            
            // Start scheduler in background thread
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("✓ Scheduler started with 2 workers\n");
            
            // ===== DEMO 1: Cancel a PENDING job =====
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("DEMO 1: Cancelling a PENDING Job");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            
            CleanupJob pendingJob = new CleanupJob();
            pendingJob.setCleanupData("/tmp", 30, ".*");
            scheduler.submitJob(pendingJob);
            System.out.println("1. Submitted cleanup job: " + pendingJob.getId());
            
            // Cancel immediately before it starts executing
            Thread.sleep(100);
            boolean cancelled = scheduler.cancelJob(pendingJob.getId());
            System.out.println("2. Cancellation requested: " + cancelled);
            
            // Check status
            Thread.sleep(500);
            JobData jobData = repository.getJobById(pendingJob.getId());
            System.out.println("3. Job status after cancellation: " + jobData.getStatus());
            System.out.println("4. Expected: CANCELLED");
            
            if (jobData.getStatus() == JobStatus.CANCELLED) {
                System.out.println("✓ PENDING job successfully cancelled\n");
            } else {
                System.out.println("✗ PENDING job cancellation failed\n");
            }
            
            // ===== DEMO 2: Cancel a RUNNING job =====
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("DEMO 2: Cancelling a RUNNING Job");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            
            ReportJob runningJob = new ReportJob();
            runningJob.setReportData("annual", "2020-01-01", "2025-12-31");
            scheduler.submitJob(runningJob);
            System.out.println("1. Submitted long-running report job: " + runningJob.getId());
            
            // Wait for job to start executing
            Thread.sleep(2000);
            JobData runningJobData = repository.getJobById(runningJob.getId());
            System.out.println("2. Job status after 2 seconds: " + runningJobData.getStatus());
            
            // Cancel while it's running
            if (runningJobData.getStatus() == JobStatus.RUNNING) {
                System.out.println("3. Job is RUNNING, requesting cancellation...");
                cancelled = scheduler.cancelJob(runningJob.getId());
                System.out.println("4. Cancellation requested: " + cancelled);
                
                // Note: Running jobs can't be cancelled immediately via database status
                // They need to check the cancellation flag via context.throwIfCancelled()
                System.out.println("5. Note: RUNNING jobs continue until they check cancellation flag");
                System.out.println("   Jobs check via context.throwIfCancelled() during execution");
            } else {
                System.out.println("3. Job already completed, cannot cancel running job");
            }
            
            // Wait for job to complete (or be interrupted)
            Thread.sleep(10000);
            runningJobData = repository.getJobById(runningJob.getId());
            System.out.println("6. Final job status: " + runningJobData.getStatus());
            
            if (runningJobData.getStatus() == JobStatus.CANCELLED) {
                System.out.println("✓ RUNNING job successfully cancelled (job checked flag)\n");
            } else if (runningJobData.getStatus() == JobStatus.SUCCESS) {
                System.out.println("⚠ Job completed before checking cancellation flag\n");
            } else {
                System.out.println("✗ Unexpected job status\n");
            }
            
            // ===== DEMO 3: Attempt to cancel completed job =====
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("DEMO 3: Attempting to Cancel a Completed Job");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            
            CleanupJob completedJob = new CleanupJob();
            completedJob.setCleanupData("/tmp", 7, ".*");
            scheduler.submitJob(completedJob);
            System.out.println("1. Submitted quick cleanup job: " + completedJob.getId());
            
            // Wait for job to complete
            Thread.sleep(8000);
            JobData completedJobData = repository.getJobById(completedJob.getId());
            System.out.println("2. Job status after execution: " + completedJobData.getStatus());
            
            // Try to cancel completed job
            if (completedJobData.getStatus() == JobStatus.SUCCESS) {
                System.out.println("3. Job is already completed, attempting cancellation...");
                cancelled = scheduler.cancelJob(completedJob.getId());
                System.out.println("4. Cancellation result: " + cancelled);
                System.out.println("5. Expected: false (cannot cancel completed jobs)");
                
                if (!cancelled) {
                    System.out.println("✓ Correctly prevented cancellation of completed job\n");
                } else {
                    System.out.println("✗ Unexpectedly cancelled completed job\n");
                }
            }
            
            // ===== SUMMARY =====
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("CANCELLATION SUMMARY");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
            
            System.out.println("Key Points:");
            System.out.println("1. ✓ PENDING jobs can be cancelled immediately");
            System.out.println("2. ✓ RUNNING jobs require cooperation from the job implementation");
            System.out.println("3. ✓ Jobs must call context.throwIfCancelled() to respect cancellation");
            System.out.println("4. ✓ Completed/Failed jobs cannot be cancelled");
            System.out.println("5. ✓ Cancellation is thread-safe using AtomicBoolean\n");
            
            System.out.println("How to Make Jobs Cancellable:");
            System.out.println("- Call context.throwIfCancelled() at regular intervals");
            System.out.println("- Especially in loops and before long operations");
            System.out.println("- Throw InterruptedException when cancelled");
            System.out.println("- Worker catches InterruptedException and marks job CANCELLED\n");
            
            // Shutdown
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            
            System.out.println("╔══════════════════════════════════════════════════════╗");
            System.out.println("║           CANCELLATION DEMO COMPLETE                 ║");
            System.out.println("╚══════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            System.err.println("✗ Demo failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

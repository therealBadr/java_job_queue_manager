package com.jobqueue.app;

import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.FailingJob;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates the Dead Letter Queue (DLQ) lifecycle:
 * 1. Submit jobs that fail
 * 2. Wait for them to exhaust retries and move to DLQ
 * 3. Display DLQ contents
 * 4. Replay selected jobs from DLQ
 * 5. Verify successful execution after replay
 */
public class DLQReplayDemo {
    
    private static final int TOTAL_JOBS = 20;
    private static final int JOBS_TO_REPLAY = 5;
    private static final int WORKER_COUNT = 4;
    
    private static Database database;
    private static JobRepository repository;
    private static Scheduler scheduler;
    private static Thread schedulerThread;
    
    public static void main(String[] args) {
        try {
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║        Dead Letter Queue (DLQ) Replay Demonstration           ║");
            System.out.println("╚════════════════════════════════════════════════════════════════╝");
            System.out.println();
            
            // Step 1: Initialize components
            initializeComponents();
            
            // Step 2: Submit failing jobs
            List<String> jobIds = submitFailingJobs();
            
            // Step 3: Wait for jobs to fail and move to DLQ
            waitForJobsToFail();
            
            // Step 4: Display DLQ contents
            List<JobRepository.JobData> dlqJobs = displayDLQContents();
            
            // Step 5: Select jobs to replay
            List<String> jobsToReplay = selectJobsForReplay(dlqJobs);
            
            // Step 6: Fix the jobs (mark them as fixed)
            fixJobs(jobsToReplay);
            
            // Step 7: Replay jobs from DLQ
            replayJobs(jobsToReplay);
            
            // Step 8: Wait for replayed jobs to complete
            waitForReplayedJobs();
            
            // Step 9: Verify successful execution
            verifyReplayResults(jobsToReplay);
            
            // Step 10: Display final DLQ state
            displayFinalDLQState();
            
            // Step 11: Cleanup
            cleanup();
            
            System.out.println();
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║        DLQ Replay Demo Completed Successfully!                ║");
            System.out.println("╚════════════════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            System.err.println("Error during DLQ replay demo: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Initialize database, repository, and scheduler
     */
    private static void initializeComponents() throws Exception {
        System.out.println("📋 Step 1: Initializing components...");
        
        // Initialize database
        database = new Database();
        database.initialize();
        System.out.println("   ✓ Database initialized");
        
        // Initialize repository
        repository = new JobRepository(database);
        int recovered = repository.recoverCrashedJobs();
        System.out.println("   ✓ Repository initialized (recovered " + recovered + " crashed jobs)");
        
        // Clear any previous fixed jobs
        FailingJob.clearFixedJobs();
        System.out.println("   ✓ Cleared previous fixed jobs");
        
        // Initialize scheduler
        scheduler = new Scheduler(WORKER_COUNT, repository);
        schedulerThread = new Thread(() -> {
            try {
                scheduler.start();
            } catch (Exception e) {
                System.err.println("Scheduler error: " + e.getMessage());
            }
        }, "Scheduler-Thread");
        schedulerThread.setDaemon(false);
        schedulerThread.start();
        System.out.println("   ✓ Scheduler started with " + WORKER_COUNT + " workers");
        System.out.println();
        
        // Give scheduler time to fully start
        Thread.sleep(500);
    }
    
    /**
     * Submit 20 jobs that will fail
     */
    private static List<String> submitFailingJobs() throws Exception {
        System.out.println("📋 Step 2: Submitting " + TOTAL_JOBS + " failing jobs...");
        
        List<String> jobIds = new ArrayList<>();
        
        for (int i = 0; i < TOTAL_JOBS; i++) {
            String taskName = "Task-" + i;
            String reason = "Simulated failure #" + i;
            
            FailingJob job = new FailingJob();
            job.setFailureData(taskName, reason);
            
            scheduler.submitJob(job);
            String jobId = job.getId();
            jobIds.add(jobId);
            
            if ((i + 1) % 5 == 0) {
                System.out.println("   ✓ Submitted " + (i + 1) + "/" + TOTAL_JOBS + " jobs");
            }
        }
        
        System.out.println("   ✓ All " + TOTAL_JOBS + " failing jobs submitted");
        System.out.println("   ℹ Each job configured with maxRetries=2");
        System.out.println();
        
        return jobIds;
    }
    
    /**
     * Wait for all jobs to fail and move to DLQ
     */
    private static void waitForJobsToFail() throws Exception {
        System.out.println("📋 Step 3: Waiting for jobs to fail and move to DLQ...");
        System.out.println("   ℹ Jobs will fail immediately, retry twice, then move to DLQ");
        System.out.println();
        
        int maxWaitSeconds = 120;
        int waited = 0;
        int lastDlqCount = 0;
        
        while (waited < maxWaitSeconds) {
            int queueDepth = repository.getQueueDepth();
            Map<String, Object> metrics = repository.getMetrics();
            long activeJobsLong = (long) metrics.getOrDefault("active_jobs", 0L);
            int activeJobs = (int) activeJobsLong;
            
            List<JobRepository.JobData> dlqJobs = repository.getDLQJobs(100);
            int dlqCount = dlqJobs.size();
            
            if (dlqCount > lastDlqCount) {
                System.out.println("   📊 Progress: " + dlqCount + " jobs in DLQ (Queue: " + queueDepth + ", Active: " + activeJobs + ")");
                lastDlqCount = dlqCount;
            }
            
            // Check if all jobs are in DLQ (should be TOTAL_JOBS in DLQ)
            if (dlqCount >= TOTAL_JOBS && queueDepth == 0 && activeJobs == 0) {
                System.out.println();
                System.out.println("   ✓ All " + TOTAL_JOBS + " jobs have moved to DLQ");
                System.out.println("   ✓ Waited " + waited + " seconds for all failures");
                break;
            }
            
            Thread.sleep(2000);
            waited += 2;
        }
        
        if (waited >= maxWaitSeconds) {
            System.out.println();
            System.out.println("   ⚠ Timeout reached, proceeding with current DLQ state...");
        }
        
        System.out.println();
    }
    
    /**
     * Display DLQ contents
     */
    private static List<JobRepository.JobData> displayDLQContents() throws Exception {
        System.out.println("📋 Step 4: Displaying DLQ contents...");
        System.out.println();
        
        List<JobRepository.JobData> dlqJobs = repository.getDLQJobs(100);
        
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Dead Letter Queue Contents                  ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Total Jobs in DLQ:   %3d                                      ║%n", dlqJobs.size());
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        
        for (int i = 0; i < Math.min(10, dlqJobs.size()); i++) {
            JobRepository.JobData job = dlqJobs.get(i);
            String truncatedId = job.getId().substring(0, Math.min(36, job.getId().length()));
            String truncatedError = job.getErrorMessage();
            if (truncatedError != null && truncatedError.length() > 35) {
                truncatedError = truncatedError.substring(0, 32) + "...";
            }
            System.out.printf("║  %2d. %s %-37s ║%n", (i + 1), 
                truncatedId.substring(0, 8) + "...", 
                truncatedError);
        }
        
        if (dlqJobs.size() > 10) {
            System.out.printf("║  ... and %d more jobs                                         ║%n", 
                dlqJobs.size() - 10);
        }
        
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        return dlqJobs;
    }
    
    /**
     * Select jobs for replay
     */
    private static List<String> selectJobsForReplay(List<JobRepository.JobData> dlqJobs) {
        System.out.println("📋 Step 5: Selecting " + JOBS_TO_REPLAY + " jobs for replay...");
        
        List<String> selectedJobs = new ArrayList<>();
        
        for (int i = 0; i < Math.min(JOBS_TO_REPLAY, dlqJobs.size()); i++) {
            String jobId = dlqJobs.get(i).getId();
            selectedJobs.add(jobId);
            System.out.println("   ✓ Selected job " + (i + 1) + ": " + jobId.substring(0, 8) + "...");
        }
        
        System.out.println("   ✓ Selected " + selectedJobs.size() + " jobs for replay");
        System.out.println();
        
        return selectedJobs;
    }
    
    /**
     * Fix the jobs by marking them as fixed
     */
    private static void fixJobs(List<String> jobIds) {
        System.out.println("📋 Step 6: Fixing selected jobs...");
        System.out.println("   ℹ Marking jobs as 'fixed' so they will succeed on replay");
        
        for (String jobId : jobIds) {
            FailingJob.fixJob(jobId);
        }
        
        System.out.println("   ✓ Fixed " + jobIds.size() + " jobs");
        System.out.println("   ✓ These jobs will now execute successfully");
        System.out.println();
    }
    
    /**
     * Replay jobs from DLQ
     */
    private static void replayJobs(List<String> jobIds) throws Exception {
        System.out.println("📋 Step 7: Replaying jobs from DLQ...");
        
        int successCount = 0;
        int failCount = 0;
        
        for (int i = 0; i < jobIds.size(); i++) {
            String jobId = jobIds.get(i);
            boolean replayed = repository.replayFromDLQ(jobId);
            
            if (replayed) {
                successCount++;
                System.out.println("   ✓ Replayed job " + (i + 1) + "/" + jobIds.size() + ": " + jobId.substring(0, 8) + "...");
            } else {
                failCount++;
                System.out.println("   ✗ Failed to replay job " + (i + 1) + "/" + jobIds.size() + ": " + jobId.substring(0, 8) + "...");
            }
        }
        
        System.out.println();
        System.out.println("   ✓ Replay summary: " + successCount + " succeeded, " + failCount + " failed");
        System.out.println("   ℹ Replayed jobs are now back in the queue with PENDING status");
        System.out.println();
    }
    
    /**
     * Wait for replayed jobs to complete
     */
    private static void waitForReplayedJobs() throws Exception {
        System.out.println("📋 Step 8: Waiting for replayed jobs to complete...");
        
        int maxWaitSeconds = 60;
        int waited = 0;
        
        while (waited < maxWaitSeconds) {
            int queueDepth = repository.getQueueDepth();
            Map<String, Object> metrics = repository.getMetrics();
            long activeJobsLong = (long) metrics.getOrDefault("active_jobs", 0L);
            int activeJobs = (int) activeJobsLong;
            
            System.out.print("\r   ⏳ Queue depth: " + queueDepth + ", Active: " + activeJobs + " (waited " + waited + "s)");
            
            if (queueDepth == 0 && activeJobs == 0) {
                System.out.println();
                System.out.println("   ✓ All replayed jobs have completed");
                break;
            }
            
            Thread.sleep(1000);
            waited++;
        }
        
        if (waited >= maxWaitSeconds) {
            System.out.println();
            System.out.println("   ⚠ Timeout reached, proceeding with verification...");
        }
        
        System.out.println();
    }
    
    /**
     * Verify successful execution after replay
     */
    private static void verifyReplayResults(List<String> replayedJobIds) throws Exception {
        System.out.println("📋 Step 9: Verifying replay results...");
        System.out.println();
        
        int successCount = 0;
        int failedCount = 0;
        int otherCount = 0;
        
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Replayed Job Results                        ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        
        for (int i = 0; i < replayedJobIds.size(); i++) {
            String jobId = replayedJobIds.get(i);
            JobRepository.JobData jobData = repository.getJobById(jobId);
            
            String statusIcon;
            String statusText;
            JobStatus status = (jobData != null) ? jobData.getStatus() : null;
            
            if (status == null) {
                statusIcon = "❓";
                statusText = "NOT_FOUND";
                otherCount++;
            } else {
                switch (status) {
                    case SUCCESS:
                        statusIcon = "✅";
                        statusText = "SUCCESS  ";
                        successCount++;
                        break;
                    case FAILED:
                        statusIcon = "❌";
                        statusText = "FAILED   ";
                        failedCount++;
                        break;
                    case CANCELLED:
                        statusIcon = "🚫";
                        statusText = "CANCELLED";
                        otherCount++;
                        break;
                    default:
                        statusIcon = "⏳";
                        statusText = status.toString();
                        otherCount++;
                        break;
                }
            }
            
            String truncatedId = jobId.substring(0, Math.min(36, jobId.length()));
            System.out.printf("║  %s Job %d: %s %s                    ║%n", 
                statusIcon, 
                (i + 1), 
                truncatedId.substring(0, 8) + "...",
                statusText);
        }
        
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Success: %2d   Failed: %2d   Other: %2d                         ║%n", 
            successCount, failedCount, otherCount);
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        // Verification checks
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Verification Checks                         ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        
        boolean check1 = (successCount == replayedJobIds.size());
        System.out.printf("║  %-60s %s  ║%n", 
            "All replayed jobs executed successfully", 
            check1 ? "✓" : "✗");
        System.out.printf("║    (%d/%d jobs succeeded)%39s║%n",
            successCount, replayedJobIds.size(), "");
        
        boolean check2 = (failedCount == 0);
        System.out.printf("║  %-60s %s  ║%n", 
            "No replayed jobs failed", 
            check2 ? "✓" : "✗");
        System.out.printf("║    (%d failures after replay)%38s║%n",
            failedCount, "");
        
        int fixedJobCount = FailingJob.getFixedJobCount();
        boolean check3 = (fixedJobCount == replayedJobIds.size());
        System.out.printf("║  %-60s %s  ║%n", 
            "All jobs properly marked as fixed", 
            check3 ? "✓" : "✗");
        System.out.printf("║    (%d jobs fixed)%47s║%n",
            fixedJobCount, "");
        
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        if (check1 && check2 && check3) {
            System.out.println("✅ All verification checks passed!");
            System.out.println("   DLQ replay mechanism is working correctly:");
            System.out.println("   - Jobs moved to DLQ after exhausting retries");
            System.out.println("   - Jobs successfully replayed from DLQ");
            System.out.println("   - Fixed jobs executed successfully");
        } else {
            System.out.println("⚠️  Some verification checks failed!");
            if (!check1) {
                System.out.println("   - Not all replayed jobs succeeded");
            }
            if (!check2) {
                System.out.println("   - Some replayed jobs still failed");
            }
            if (!check3) {
                System.out.println("   - Job fix count mismatch");
            }
        }
        System.out.println();
    }
    
    /**
     * Display final DLQ state
     */
    private static void displayFinalDLQState() throws Exception {
        System.out.println("📋 Step 10: Displaying final DLQ state...");
        
        List<JobRepository.JobData> remainingDlqJobs = repository.getDLQJobs(100);
        
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Final DLQ State                             ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Original DLQ count:      %3d                                   ║%n", TOTAL_JOBS);
        System.out.printf("║  Jobs replayed:           %3d                                   ║%n", JOBS_TO_REPLAY);
        System.out.printf("║  Remaining in DLQ:        %3d                                   ║%n", remainingDlqJobs.size());
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
    
    /**
     * Cleanup resources
     */
    private static void cleanup() throws Exception {
        System.out.println("📋 Step 11: Cleaning up resources...");
        
        if (scheduler != null) {
            scheduler.shutdown();
            System.out.println("   ✓ Scheduler stopped");
        }
        
        if (schedulerThread != null && schedulerThread.isAlive()) {
            schedulerThread.join(5000);
            if (schedulerThread.isAlive()) {
                System.out.println("   ⚠ Scheduler thread still running after 5s");
            } else {
                System.out.println("   ✓ Scheduler thread terminated");
            }
        }
        
        if (database != null) {
            database.close();
            System.out.println("   ✓ Database closed");
        }
        
        // Clear fixed jobs
        FailingJob.clearFixedJobs();
        System.out.println("   ✓ Cleared fixed jobs");
        
        System.out.println("   ✓ All resources cleaned up");
    }
}

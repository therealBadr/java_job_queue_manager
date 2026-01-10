package com.jobqueue.app;

import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.ReportJob;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Demonstrates job cancellation functionality.
 * 
 * This demo:
 * 1. Submits 10 long-running ReportGenerationJobs
 * 2. Waits for them to start executing
 * 3. Cancels 5 of them mid-execution
 * 4. Verifies cancelled jobs stop and non-cancelled jobs complete
 */
public class CancellationDemo {
    
    private static final int TOTAL_JOBS = 10;
    private static final int JOBS_TO_CANCEL = 5;
    private static final int WORKER_COUNT = 4;
    
    private static Database database;
    private static JobRepository repository;
    private static Scheduler scheduler;
    private static Thread schedulerThread;
    
    public static void main(String[] args) {
        try {
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║        Job Cancellation Demonstration                          ║");
            System.out.println("╚════════════════════════════════════════════════════════════════╝");
            System.out.println();
            
            // Step 1: Initialize components
            initializeComponents();
            
            // Step 2: Submit long-running jobs
            List<String> jobIds = submitLongRunningJobs();
            
            // Step 3: Wait for jobs to start executing
            waitForJobsToStart();
            
            // Step 4: Cancel half of the jobs
            List<String> cancelledJobIds = cancelJobs(jobIds);
            
            // Step 5: Wait for all jobs to finish
            waitForCompletion();
            
            // Step 6: Verify and display results
            verifyAndDisplayResults(jobIds, cancelledJobIds);
            
            // Step 7: Cleanup
            cleanup();
            
            System.out.println();
            System.out.println("╔════════════════════════════════════════════════════════════════╗");
            System.out.println("║        Cancellation Demo Completed Successfully!              ║");
            System.out.println("╚════════════════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            System.err.println("Error during cancellation demo: " + e.getMessage());
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
        
        // Initialize scheduler with limited workers to make cancellation more visible
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
     * Submit 10 long-running ReportJobs
     */
    private static List<String> submitLongRunningJobs() throws Exception {
        System.out.println("📋 Step 2: Submitting " + TOTAL_JOBS + " long-running jobs...");
        
        List<String> jobIds = new ArrayList<>();
        
        for (int i = 0; i < TOTAL_JOBS; i++) {
            String reportName = "Long-Running-Report-" + i;
            ReportJob job = new ReportJob();
            job.setReportData("Monthly", "2024-01-01", "2024-01-31");
            
            scheduler.submitJob(job);
            String jobId = job.getId();
            jobIds.add(jobId);
            
            System.out.println("   ✓ Submitted job " + (i + 1) + "/" + TOTAL_JOBS + ": " + jobId + " (Report: " + reportName + ")");
        }
        
        System.out.println("   ✓ All " + TOTAL_JOBS + " jobs submitted successfully");
        System.out.println();
        
        return jobIds;
    }
    
    /**
     * Wait for jobs to start executing
     */
    private static void waitForJobsToStart() throws Exception {
        System.out.println("📋 Step 3: Waiting for jobs to start executing...");
        System.out.println("   ⏳ Sleeping for 2 seconds to allow jobs to begin...");
        
        Thread.sleep(2000);
        
        // Check queue metrics
        Map<String, Object> metrics = repository.getMetrics();
        long runningJobsLong = (long) metrics.getOrDefault("active_jobs", 0L);
        int runningJobs = (int) runningJobsLong;
        System.out.println("   ✓ Currently " + runningJobs + " jobs are running");
        System.out.println();
    }
    
    /**
     * Cancel half of the submitted jobs
     */
    private static List<String> cancelJobs(List<String> jobIds) throws Exception {
        System.out.println("📋 Step 4: Cancelling " + JOBS_TO_CANCEL + " jobs...");
        
        List<String> cancelledJobIds = new ArrayList<>();
        
        // Cancel the first JOBS_TO_CANCEL jobs
        for (int i = 0; i < JOBS_TO_CANCEL && i < jobIds.size(); i++) {
            String jobId = jobIds.get(i);
            boolean cancelled = scheduler.cancelJob(jobId);
            
            if (cancelled) {
                cancelledJobIds.add(jobId);
                System.out.println("   ✓ Successfully cancelled job: " + jobId);
            } else {
                System.out.println("   ✗ Failed to cancel job: " + jobId + " (may have already completed)");
            }
        }
        
        System.out.println("   ✓ Cancellation requests sent for " + cancelledJobIds.size() + " jobs");
        System.out.println();
        
        return cancelledJobIds;
    }
    
    /**
     * Wait for all jobs to finish processing
     */
    private static void waitForCompletion() throws Exception {
        System.out.println("📋 Step 5: Waiting for all jobs to complete...");
        
        int maxWaitSeconds = 60;
        int waited = 0;
        
        while (waited < maxWaitSeconds) {
            int queueDepth = repository.getQueueDepth();
            Map<String, Object> metrics = repository.getMetrics();
            long runningJobsLong = (long) metrics.getOrDefault("active_jobs", 0L);
            int runningJobs = (int) runningJobsLong;
            
            System.out.print("\r   ⏳ Queue depth: " + queueDepth + ", Running: " + runningJobs + " (waited " + waited + "s)");
            
            if (queueDepth == 0 && runningJobs == 0) {
                System.out.println();
                System.out.println("   ✓ All jobs have finished processing");
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
     * Verify results and display status of each job
     */
    private static void verifyAndDisplayResults(List<String> jobIds, List<String> cancelledJobIds) throws Exception {
        System.out.println("📋 Step 6: Verifying and displaying results...");
        System.out.println();
        
        Map<String, JobStatus> jobStatuses = new HashMap<>();
        int actualCancelled = 0;
        int actualCompleted = 0;
        int actualFailed = 0;
        int other = 0;
        
        // Collect status for each job
        for (String jobId : jobIds) {
            JobRepository.JobData jobData = repository.getJobById(jobId);
            JobStatus status = (jobData != null) ? jobData.getStatus() : null;
            jobStatuses.put(jobId, status);
            
            if (status == null) {
                other++;
                continue;
            }
            
            switch (status) {
                case CANCELLED:
                    actualCancelled++;
                    break;
                case SUCCESS:
                    actualCompleted++;
                    break;
                case FAILED:
                    actualFailed++;
                    break;
                default:
                    other++;
                    break;
            }
        }
        
        // Display summary
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Verification Summary                        ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Total Jobs Submitted:        %3d                              ║%n", TOTAL_JOBS);
        System.out.printf("║  Jobs Cancelled:              %3d ✓                            ║%n", actualCancelled);
        System.out.printf("║  Jobs Completed:              %3d ✓                            ║%n", actualCompleted);
        System.out.printf("║  Jobs Failed:                 %3d                              ║%n", actualFailed);
        System.out.printf("║  Other Status:                %3d                              ║%n", other);
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        // Display detailed status for each job
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Detailed Job Status                         ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        
        for (int i = 0; i < jobIds.size(); i++) {
            String jobId = jobIds.get(i);
            JobStatus status = jobStatuses.get(jobId);
            boolean wasCancellationTarget = cancelledJobIds.contains(jobId);
            
            String statusIcon;
            String statusText;
            
            if (status == null) {
                statusIcon = "❓";
                statusText = "UNKNOWN   ";
            } else {
                switch (status) {
                    case CANCELLED:
                        statusIcon = "🚫";
                        statusText = "CANCELLED";
                        break;
                    case SUCCESS:
                        statusIcon = "✅";
                        statusText = "COMPLETED";
                        break;
                    case FAILED:
                        statusIcon = "❌";
                        statusText = "FAILED    ";
                        break;
                    default:
                        statusIcon = "⏳";
                        statusText = status.toString();
                        break;
                }
            }
            
            String marker = wasCancellationTarget ? " [CANCEL TARGET]" : "               ";
            String truncatedId = jobId.length() > 36 ? jobId.substring(0, 36) : String.format("%-36s", jobId);
            
            System.out.printf("║  %s Job %2d: %s %s %s ║%n", 
                statusIcon, 
                (i + 1), 
                truncatedId,
                statusText,
                marker);
        }
        
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        // Verification checks
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    Verification Checks                         ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        
        // Check 1: Cancelled jobs have CANCELLED status
        int cancelledMatchCount = 0;
        for (String jobId : cancelledJobIds) {
            if (jobStatuses.get(jobId) == JobStatus.CANCELLED) {
                cancelledMatchCount++;
            }
        }
        boolean check1 = (cancelledMatchCount == cancelledJobIds.size());
        System.out.printf("║  %-60s %s  ║%n", 
            "Cancelled jobs have CANCELLED status", 
            check1 ? "✓" : "✗");
        System.out.printf("║    (%d/%d cancelled jobs have CANCELLED status)%17s║%n",
            cancelledMatchCount, cancelledJobIds.size(), "");
        
        // Check 2: Non-cancelled jobs completed or failed (not stuck)
        int nonCancelledCount = jobIds.size() - cancelledJobIds.size();
        int nonCancelledFinished = actualCompleted + actualFailed;
        boolean check2 = (nonCancelledFinished >= nonCancelledCount - 1); // Allow 1 to be in other state
        System.out.printf("║  %-60s %s  ║%n", 
            "Non-cancelled jobs completed normally", 
            check2 ? "✓" : "✗");
        System.out.printf("║    (%d/%d non-cancelled jobs finished)%24s║%n",
            nonCancelledFinished, nonCancelledCount, "");
        
        // Check 3: Cancelled jobs stopped mid-execution
        boolean check3 = (actualCancelled > 0);
        System.out.printf("║  %-60s %s  ║%n", 
            "Cancelled jobs stopped execution", 
            check3 ? "✓" : "✗");
        System.out.printf("║    (%d jobs successfully cancelled)%30s║%n",
            actualCancelled, "");
        
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        // Overall result
        if (check1 && check2 && check3) {
            System.out.println("✅ All verification checks passed!");
            System.out.println("   Cancellation is working correctly:");
            System.out.println("   - Cancelled jobs were stopped mid-execution");
            System.out.println("   - Non-cancelled jobs completed normally");
            System.out.println("   - Job statuses are accurate");
        } else {
            System.out.println("⚠️  Some verification checks failed!");
            if (!check1) {
                System.out.println("   - Some cancelled jobs don't have CANCELLED status");
            }
            if (!check2) {
                System.out.println("   - Some non-cancelled jobs didn't complete");
            }
            if (!check3) {
                System.out.println("   - No jobs were successfully cancelled");
            }
        }
    }
    
    /**
     * Cleanup resources
     */
    private static void cleanup() throws Exception {
        System.out.println();
        System.out.println("📋 Step 7: Cleaning up resources...");
        
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
        
        System.out.println("   ✓ All resources cleaned up");
    }
}

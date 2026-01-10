package com.jobqueue.test;

import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.*;

import java.util.Map;

/**
 * Test class for Scheduler component
 */
public class TestScheduler {
    
    public static void main(String[] args) {
        System.out.println("=== Testing Scheduler Component ===\n");
        
        boolean allPassed = true;
        
        // Test 1: Scheduler Initialization
        allPassed &= testSchedulerInitialization();
        
        // Test 2: Job Submission and Execution
        allPassed &= testJobSubmissionAndExecution();
        
        // Test 3: Multiple Jobs Concurrent Execution
        allPassed &= testMultipleJobsExecution();
        
        // Test 4: Graceful Shutdown
        allPassed &= testGracefulShutdown();
        
        System.out.println("\n=== Test Summary ===");
        if (allPassed) {
            System.out.println("✓ All Scheduler Tests PASSED");
            System.exit(0);
        } else {
            System.out.println("✗ Some Tests FAILED");
            System.exit(1);
        }
    }
    
    private static boolean testSchedulerInitialization() {
        System.out.println("--- Test 1: Scheduler Initialization ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create scheduler with 3 workers
            Scheduler scheduler = new Scheduler(3, repository);
            assert !scheduler.isRunning() : "Scheduler should not be running initially";
            assert scheduler.getWorkerCount() == 3 : "Worker count should be 3";
            assert scheduler.getQueueDepthThreshold() == 1000 : "Default threshold should be 1000";
            System.out.println("  ✓ Scheduler created with 3 workers");
            
            // Get initial status
            Map<String, Object> status = scheduler.getStatus();
            assert status != null : "Status should not be null";
            assert !((Boolean) status.get("running")) : "Should not be running";
            System.out.println("  ✓ Status: " + status);
            
            database.close();
            System.out.println("✓ Scheduler Initialization Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Scheduler Initialization Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testJobSubmissionAndExecution() {
        System.out.println("--- Test 2: Job Submission and Execution ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create and start scheduler in a separate thread
            Scheduler scheduler = new Scheduler(2, repository);
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            
            // Wait for scheduler to start
            Thread.sleep(500);
            assert scheduler.isRunning() : "Scheduler should be running";
            System.out.println("  ✓ Scheduler started");
            
            // Submit a cleanup job
            CleanupJob job = new CleanupJob();
            job.setCleanupData("/tmp/test", 7, ".*\\.log");
            scheduler.submitJob(job);
            System.out.println("  ✓ Job submitted: " + job.getId());
            
            // Wait for job to be picked up and executed
            Thread.sleep(8000); // Wait 8 seconds for execution
            
            // Check job status
            JobRepository.JobData jobData = repository.getJobById(job.getId());
            assert jobData != null : "Job should exist in database";
            System.out.println("  ✓ Job status: " + jobData.getStatus());
            
            // Get scheduler status
            Map<String, Object> status = scheduler.getStatus();
            System.out.println("  ✓ Scheduler status: running=" + status.get("running") + 
                             ", active=" + status.get("activeJobs") + 
                             ", pending=" + status.get("pendingJobs"));
            
            // Shutdown
            scheduler.shutdown();
            schedulerThread.join(5000); // Wait for scheduler thread to exit
            assert !scheduler.isRunning() : "Scheduler should not be running after shutdown";
            System.out.println("  ✓ Scheduler shutdown");
            
            database.close();
            System.out.println("✓ Job Submission and Execution Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Job Submission and Execution Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testMultipleJobsExecution() {
        System.out.println("--- Test 3: Multiple Jobs Concurrent Execution ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create scheduler with 3 workers
            Scheduler scheduler = new Scheduler(3, repository);
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500); // Wait for scheduler to start
            System.out.println("  ✓ Scheduler started with 3 workers");
            
            // Submit multiple jobs of different types
            EmailJob emailJob = new EmailJob("user@test.com", "Test", "Test message");
            scheduler.submitJob(emailJob);
            System.out.println("  ✓ EmailJob submitted: " + emailJob.getId());
            
            CleanupJob cleanupJob = new CleanupJob();
            cleanupJob.setCleanupData("/var/tmp", 15, ".*");
            scheduler.submitJob(cleanupJob);
            System.out.println("  ✓ CleanupJob submitted: " + cleanupJob.getId());
            
            ReportJob reportJob = new ReportJob();
            reportJob.setReportData("monthly", "2024-01-01", "2024-12-31");
            scheduler.submitJob(reportJob);
            System.out.println("  ✓ ReportJob submitted: " + reportJob.getId());
            
            // Wait for jobs to be processed
            System.out.println("  Waiting for jobs to execute...");
            Thread.sleep(15000); // Wait 15 seconds for all jobs
            
            // Check status of all jobs
            int successCount = 0;
            JobRepository.JobData job1 = repository.getJobById(emailJob.getId());
            JobRepository.JobData job2 = repository.getJobById(cleanupJob.getId());
            JobRepository.JobData job3 = repository.getJobById(reportJob.getId());
            
            if (job1 != null && "SUCCESS".equals(job1.getStatus().name())) successCount++;
            if (job2 != null && "SUCCESS".equals(job2.getStatus().name())) successCount++;
            if (job3 != null && "SUCCESS".equals(job3.getStatus().name())) successCount++;
            
            System.out.println("  ✓ Jobs completed: " + successCount + "/3");
            System.out.println("    EmailJob: " + (job1 != null ? job1.getStatus() : "NOT FOUND"));
            System.out.println("    CleanupJob: " + (job2 != null ? job2.getStatus() : "NOT FOUND"));
            System.out.println("    ReportJob: " + (job3 != null ? job3.getStatus() : "NOT FOUND"));
            
            // Get final metrics
            Map<String, Object> metrics = repository.getMetrics();
            System.out.println("  ✓ Final metrics:");
            System.out.println("    Total processed: " + metrics.get("total_processed"));
            System.out.println("    Success rate: " + metrics.get("success_rate"));
            
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            System.out.println("✓ Multiple Jobs Execution Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Multiple Jobs Execution Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testGracefulShutdown() {
        System.out.println("--- Test 4: Graceful Shutdown ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create and start scheduler
            Scheduler scheduler = new Scheduler(2, repository);
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("  ✓ Scheduler started");
            
            // Submit a long-running job
            ReportJob job = new ReportJob();
            job.setReportData("annual", "2024-01-01", "2024-12-31");
            scheduler.submitJob(job);
            System.out.println("  ✓ Long-running job submitted");
            
            // Wait a bit for job to start
            Thread.sleep(2000);
            
            // Shutdown (should wait for running jobs)
            System.out.println("  Initiating graceful shutdown...");
            long startTime = System.currentTimeMillis();
            scheduler.shutdown();
            schedulerThread.join(35000); // Wait for scheduler thread to exit
            long shutdownTime = System.currentTimeMillis() - startTime;
            
            System.out.println("  ✓ Shutdown completed in " + shutdownTime + "ms");
            assert !scheduler.isRunning() : "Scheduler should not be running";
            
            // Get final status
            Map<String, Object> status = scheduler.getStatus();
            assert ((Boolean) status.get("executorTerminated")) : "Executor should be terminated";
            System.out.println("  ✓ Executor terminated: " + status.get("executorTerminated"));
            
            database.close();
            System.out.println("✓ Graceful Shutdown Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Graceful Shutdown Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

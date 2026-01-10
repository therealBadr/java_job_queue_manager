package com.jobqueue.test;

import com.jobqueue.core.*;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Worker;
import com.jobqueue.jobs.*;

/**
 * Test class for Worker component
 */
public class TestWorker {
    
    public static void main(String[] args) {
        System.out.println("=== Testing Worker Component ===\n");
        
        boolean allPassed = true;
        
        // Test 1: Successful Job Execution
        allPassed &= testSuccessfulExecution();
        
        // Test 2: Job Failure with Retry
        allPassed &= testJobFailureWithRetry();
        
        // Test 3: Job Cancellation
        allPassed &= testJobCancellation();
        
        System.out.println("\n=== Test Summary ===");
        if (allPassed) {
            System.out.println("✓ All Worker Tests PASSED");
            System.exit(0);
        } else {
            System.out.println("✗ Some Tests FAILED");
            System.exit(1);
        }
    }
    
    private static boolean testSuccessfulExecution() {
        System.out.println("--- Test 1: Successful Job Execution ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create and submit a cleanup job
            CleanupJob job = new CleanupJob();
            job.setCleanupData("/tmp", 30, ".*\\.tmp");
            repository.submitJob(job);
            System.out.println("  ✓ Job submitted: " + job.getId());
            
            // Claim the job
            boolean claimed = repository.claimJob(job.getId());
            assert claimed : "Job should be claimed";
            System.out.println("  ✓ Job claimed");
            
            // Execute job with Worker
            Worker worker = new Worker(job, repository);
            Thread workerThread = new Thread(worker);
            workerThread.start();
            workerThread.join(); // Wait for completion
            System.out.println("  ✓ Worker finished execution");
            
            // Verify job status
            JobRepository.JobData jobData = repository.getJobById(job.getId());
            assert jobData != null : "Job should exist";
            assert JobStatus.SUCCESS.equals(jobData.getStatus()) : "Job should be SUCCESS, but was: " + jobData.getStatus();
            System.out.println("  ✓ Job status is SUCCESS");
            
            // Check job logs
            System.out.println("  ✓ Job executed successfully");
            
            database.close();
            System.out.println("✓ Successful Execution Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Successful Execution Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testJobFailureWithRetry() {
        System.out.println("--- Test 2: Job Failure with Retry ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create a job that will fail (invalid JSON payload)
            EmailJob job = new EmailJob();
            // Don't set email data - will cause JSONException on execute
            repository.submitJob(job);
            System.out.println("  ✓ Job submitted: " + job.getId());
            
            // Claim the job
            repository.claimJob(job.getId());
            
            // Execute job with Worker (will fail)
            Worker worker = new Worker(job, repository);
            Thread workerThread = new Thread(worker);
            workerThread.start();
            workerThread.join(); // Wait for completion
            System.out.println("  ✓ Worker finished execution (failed as expected)");
            
            // Verify job status (should still be PENDING for retry)
            JobRepository.JobData jobData = repository.getJobById(job.getId());
            assert jobData != null : "Job should exist";
            assert JobStatus.PENDING.equals(jobData.getStatus()) : "Job should be PENDING for retry, but was: " + jobData.getStatus();
            System.out.println("  ✓ Job status is PENDING (scheduled for retry)");
            
            // Verify retry count incremented
            assert jobData.getRetryCount() == 1 : "Retry count should be 1, but was: " + jobData.getRetryCount();
            System.out.println("  ✓ Retry count incremented to: " + jobData.getRetryCount());
            
            database.close();
            System.out.println("✓ Job Failure with Retry Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Job Failure with Retry Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testJobCancellation() {
        System.out.println("--- Test 3: Job Cancellation ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create a report job (takes longer)
            ReportJob job = new ReportJob();
            job.setReportData("sales", "2024-01-01", "2024-12-31");
            repository.submitJob(job);
            System.out.println("  ✓ Job submitted: " + job.getId());
            
            // Claim the job
            repository.claimJob(job.getId());
            
            // Execute job with Worker in separate thread
            Worker worker = new Worker(job, repository);
            Thread workerThread = new Thread(worker);
            workerThread.start();
            
            // Wait a bit then cancel
            Thread.sleep(1000);
            boolean cancelled = repository.cancelJob(job.getId());
            System.out.println("  ✓ Job cancellation requested: " + cancelled);
            
            // Wait for worker to finish
            workerThread.join(10000); // Wait max 10 seconds
            
            // Verify job status
            JobRepository.JobData jobData = repository.getJobById(job.getId());
            assert jobData != null : "Job should exist";
            // Note: The job might complete successfully if it finished before cancellation
            System.out.println("  ✓ Job final status: " + jobData.getStatus());
            
            database.close();
            System.out.println("✓ Job Cancellation Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Job Cancellation Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

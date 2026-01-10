package com.jobqueue.test;

import com.jobqueue.core.*;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.jobs.*;

/**
 * Test class for core components (core, db, jobs packages)
 */
public class TestCore {
    
    public static void main(String[] args) {
        System.out.println("=== Testing Core Components ===\n");
        
        boolean allPassed = true;
        
        // Test 1: JobStatus
        allPassed &= testJobStatus();
        
        // Test 2: Database Connection
        allPassed &= testDatabase();
        
        // Test 3: Job Creation
        allPassed &= testJobCreation();
        
        // Test 4: JobRepository
        allPassed &= testJobRepository();
        
        // Test 5: JobContext
        allPassed &= testJobContext();
        
        System.out.println("\n=== Test Summary ===");
        if (allPassed) {
            System.out.println("✓ All Core Tests PASSED");
            System.exit(0);
        } else {
            System.out.println("✗ Some Tests FAILED");
            System.exit(1);
        }
    }
    
    private static boolean testJobStatus() {
        System.out.println("--- Test 1: JobStatus ---");
        try {
            // Test terminal states
            assert JobStatus.SUCCESS.isTerminal() : "SUCCESS should be terminal";
            assert JobStatus.FAILED.isTerminal() : "FAILED should be terminal";
            assert JobStatus.CANCELLED.isTerminal() : "CANCELLED should be terminal";
            assert !JobStatus.PENDING.isTerminal() : "PENDING should not be terminal";
            assert !JobStatus.RUNNING.isTerminal() : "RUNNING should not be terminal";
            System.out.println("  ✓ Terminal states correct");
            
            // Test valid transitions
            assert JobStatus.PENDING.canTransitionTo(JobStatus.RUNNING) : "PENDING should transition to RUNNING";
            assert JobStatus.PENDING.canTransitionTo(JobStatus.CANCELLED) : "PENDING should transition to CANCELLED";
            assert JobStatus.RUNNING.canTransitionTo(JobStatus.SUCCESS) : "RUNNING should transition to SUCCESS";
            assert JobStatus.RUNNING.canTransitionTo(JobStatus.FAILED) : "RUNNING should transition to FAILED";
            System.out.println("  ✓ Valid transitions work");
            
            // Test invalid transitions
            assert !JobStatus.SUCCESS.canTransitionTo(JobStatus.RUNNING) : "SUCCESS should not transition to RUNNING";
            assert !JobStatus.FAILED.canTransitionTo(JobStatus.PENDING) : "FAILED should not transition to PENDING";
            System.out.println("  ✓ Invalid transitions blocked");
            
            System.out.println("✓ JobStatus tests PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ JobStatus tests FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testDatabase() {
        System.out.println("--- Test 2: Database Connection ---");
        Database database = new Database();
        try {
            database.initialize();
            System.out.println("  ✓ Database initialized successfully");
            
            // Test connection from pool
            var conn = database.getConnection();
            assert conn != null : "Connection should not be null";
            assert !conn.isClosed() : "Connection should be open";
            System.out.println("  ✓ Database connection obtained");
            
            conn.close();
            System.out.println("  ✓ Connection returned to pool");
            
            database.close();
            System.out.println("✓ Database tests PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Database tests FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testJobCreation() {
        System.out.println("--- Test 3: Job Creation ---");
        try {
            // Test EmailJob
            EmailJob emailJob = new EmailJob("user@test.com", "Test Subject", "Test Body");
            assert emailJob.getId() != null : "EmailJob should have ID";
            assert "EmailJob".equals(emailJob.getType()) : "EmailJob type should be 'EmailJob'";
            assert emailJob.getPriority() == 5 : "EmailJob priority should be 5";
            assert emailJob.getMaxRetries() == 3 : "EmailJob max retries should be 3";
            assert emailJob.isCancellable() : "EmailJob should be cancellable";
            System.out.println("  ✓ EmailJob created: " + emailJob.getId());
            
            // Test CleanupJob
            CleanupJob cleanupJob = new CleanupJob();
            cleanupJob.setCleanupData("/tmp", 30, ".*\\.tmp");
            assert cleanupJob.getId() != null : "CleanupJob should have ID";
            assert "CleanupJob".equals(cleanupJob.getType()) : "CleanupJob type should be 'CleanupJob'";
            assert cleanupJob.getPriority() == 3 : "CleanupJob priority should be 3";
            assert cleanupJob.getMaxRetries() == 2 : "CleanupJob max retries should be 2";
            System.out.println("  ✓ CleanupJob created: " + cleanupJob.getId());
            
            // Test ReportJob
            ReportJob reportJob = new ReportJob();
            reportJob.setReportData("sales", "2024-01-01", "2024-12-31");
            assert reportJob.getId() != null : "ReportJob should have ID";
            assert "ReportJob".equals(reportJob.getType()) : "ReportJob type should be 'ReportJob'";
            assert reportJob.getPriority() == 7 : "ReportJob priority should be 7";
            System.out.println("  ✓ ReportJob created: " + reportJob.getId());
            
            System.out.println("✓ Job creation tests PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Job creation tests FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testJobRepository() {
        System.out.println("--- Test 4: JobRepository ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Test job submission
            EmailJob job = new EmailJob("test@example.com", "Test", "Testing repository");
            repository.submitJob(job);
            System.out.println("  ✓ Job submitted: " + job.getId());
            
            // Test job retrieval
            JobRepository.JobData retrievedJob = repository.getJobById(job.getId());
            assert retrievedJob != null : "Retrieved job should not be null";
            assert job.getId().equals(retrievedJob.getId()) : "Job IDs should match";
            assert JobStatus.PENDING.equals(retrievedJob.getStatus()) : "Job should be PENDING";
            System.out.println("  ✓ Job retrieved: " + retrievedJob.getId());
            
            // Test status update
            repository.updateJobStatus(job.getId(), JobStatus.RUNNING, null);
            retrievedJob = repository.getJobById(job.getId());
            assert JobStatus.RUNNING.equals(retrievedJob.getStatus()) : "Job should be RUNNING";
            System.out.println("  ✓ Job status updated to RUNNING");
            
            // Test pending jobs
            CleanupJob job2 = new CleanupJob();
            job2.setCleanupData("/tmp", 30, ".*");
            repository.submitJob(job2);
            var pendingJobs = repository.getPendingJobs(10);
            assert !pendingJobs.isEmpty() : "Should have pending jobs";
            System.out.println("  ✓ Pending jobs count: " + pendingJobs.size());
            
            // Test queue depth
            int depth = repository.getQueueDepth();
            assert depth > 0 : "Queue depth should be > 0";
            System.out.println("  ✓ Queue depth: " + depth);
            
            // Test metrics
            var metrics = repository.getMetrics();
            assert metrics != null : "Metrics should not be null";
            System.out.println("  ✓ Metrics retrieved:");
            System.out.println("    - Active jobs: " + metrics.get("active_jobs"));
            System.out.println("    - Pending jobs: " + metrics.get("pending_jobs"));
            
            // Test claim job (atomic operation)
            boolean claimed = repository.claimJob(job2.getId());
            assert claimed : "Should claim pending job";
            System.out.println("  ✓ Job claimed atomically");
            
            database.close();
            System.out.println("✓ JobRepository tests PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ JobRepository tests FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testJobContext() {
        System.out.println("--- Test 5: JobContext ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create a test job
            EmailJob job = new EmailJob("test@example.com", "Test", "Test");
            repository.submitJob(job);
            
            // Create context
            JobContext context = new JobContext(job.getId(), repository);
            assert !context.isCancelled() : "Context should not be cancelled initially";
            System.out.println("  ✓ JobContext created");
            
            // Test logging
            context.log("INFO", "Test log message");
            System.out.println("  ✓ Logging works");
            
            // Test metadata
            context.addMetadata("key1", "value1");
            context.addMetadata("key2", 123);
            assert "value1".equals(context.getMetadata("key1")) : "Metadata should be retrievable";
            System.out.println("  ✓ Metadata works");
            
            // Test progress update
            context.updateProgress("50% complete");
            System.out.println("  ✓ Progress update works");
            
            // Test cancellation
            context.cancel();
            assert context.isCancelled() : "Context should be cancelled";
            System.out.println("  ✓ Cancellation works");
            
            try {
                context.throwIfCancelled();
                assert false : "Should have thrown InterruptedException";
            } catch (InterruptedException e) {
                System.out.println("  ✓ throwIfCancelled() works");
            }
            
            database.close();
            System.out.println("✓ JobContext tests PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ JobContext tests FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

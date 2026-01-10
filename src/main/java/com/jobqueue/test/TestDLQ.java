package com.jobqueue.test;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;
import com.jobqueue.jobs.EmailJob;

import java.sql.SQLException;
import java.util.List;

/**
 * Test suite for Dead Letter Queue (DLQ) functionality
 */
public class TestDLQ {
    
    public static void main(String[] args) throws Exception {
        System.out.println("Starting DLQ Tests...\n");
        
        int passed = 0;
        int total = 0;
        
        // Test 1: Move job to DLQ
        total++;
        try {
            testMoveToDeadLetterQueue();
            System.out.println("✓ Test 1: Move job to DLQ - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 1: Move job to DLQ - FAILED");
            e.printStackTrace();
        }
        
        // Test 2: Get DLQ jobs
        total++;
        try {
            testGetDLQJobs();
            System.out.println("✓ Test 2: Get DLQ jobs - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 2: Get DLQ jobs - FAILED");
            e.printStackTrace();
        }
        
        // Test 3: Replay from DLQ
        total++;
        try {
            testReplayFromDLQ();
            System.out.println("✓ Test 3: Replay from DLQ - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 3: Replay from DLQ - FAILED");
            e.printStackTrace();
        }
        
        // Test 4: Clear DLQ
        total++;
        try {
            testClearDLQ();
            System.out.println("✓ Test 4: Clear DLQ - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 4: Clear DLQ - FAILED");
            e.printStackTrace();
        }
        
        // Test 5: Transaction rollback on error
        total++;
        try {
            testTransactionRollback();
            System.out.println("✓ Test 5: Transaction rollback on error - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 5: Transaction rollback on error - FAILED");
            e.printStackTrace();
        }
        
        System.out.println("\n" + passed + "/" + total + " DLQ tests passed");
        
        if (passed != total) {
            throw new RuntimeException("Some tests failed");
        }
    }
    
    private static void testMoveToDeadLetterQueue() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        // Submit a job
        EmailJob job = new EmailJob("test@example.com", "Test Subject", "Test Body");
        repo.submitJob(job);
        String jobId = job.getId();
        
        // Mark it as failed
        repo.updateJobStatus(jobId, JobStatus.FAILED, "Final error message");
        
        // Move to DLQ
        repo.moveToDeadLetterQueue(job, "Exhausted all retries");
        
        // Verify job is no longer in jobs table
        JobData jobData = repo.getJobById(jobId);
        if (jobData != null) {
            throw new AssertionError("Job should not exist in jobs table after moving to DLQ");
        }
        
        // Verify job is in DLQ
        List<JobData> dlqJobs = repo.getDLQJobs(10);
        boolean found = false;
        for (JobData dlqJob : dlqJobs) {
            if (dlqJob.getId().equals(jobId)) {
                found = true;
                break;
            }
        }
        
        if (!found) {
            throw new AssertionError("Job should be in DLQ");
        }
        
        db.close();
    }
    
    private static void testGetDLQJobs() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        // Clear DLQ first
        repo.clearDLQ();
        
        // Submit and move 3 jobs to DLQ
        for (int i = 0; i < 3; i++) {
            EmailJob job = new EmailJob("test" + i + "@example.com", "Subject " + i, "Body " + i);
            repo.submitJob(job);
            String jobId = job.getId();
            repo.updateJobStatus(jobId, JobStatus.FAILED, "Error " + i);
            repo.moveToDeadLetterQueue(job, "Error " + i);
            Thread.sleep(10); // Small delay to ensure different timestamps
        }
        
        // Get DLQ jobs with limit
        List<JobData> dlqJobs = repo.getDLQJobs(2);
        
        if (dlqJobs.size() != 2) {
            throw new AssertionError("Expected 2 jobs in DLQ, got " + dlqJobs.size());
        }
        
        // Get all DLQ jobs
        dlqJobs = repo.getDLQJobs(10);
        if (dlqJobs.size() != 3) {
            throw new AssertionError("Expected 3 jobs in DLQ, got " + dlqJobs.size());
        }
        
        db.close();
    }
    
    private static void testReplayFromDLQ() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        // Submit and move a job to DLQ
        EmailJob job = new EmailJob("replay@example.com", "Replay Subject", "Replay Body");
        repo.submitJob(job);
        String jobId = job.getId();
        repo.updateJobStatus(jobId, JobStatus.FAILED, "Original error");
        repo.moveToDeadLetterQueue(job, "Failed after retries");
        
        // Replay from DLQ
        boolean replayed = repo.replayFromDLQ(jobId);
        
        if (!replayed) {
            throw new AssertionError("Replay should return true");
        }
        
        // Verify job is back in jobs table
        JobData jobData = repo.getJobById(jobId);
        if (jobData == null) {
            throw new AssertionError("Job should exist in jobs table after replay");
        }
        
        // Verify status is PENDING
        if (jobData.getStatus() != JobStatus.PENDING) {
            throw new AssertionError("Replayed job should have PENDING status, got " + jobData.getStatus());
        }
        
        // Verify retry count is reset
        if (jobData.getRetryCount() != 0) {
            throw new AssertionError("Replayed job should have retry_count = 0, got " + jobData.getRetryCount());
        }
        
        // Verify error message is cleared
        if (jobData.getErrorMessage() != null) {
            throw new AssertionError("Replayed job should have null error_message");
        }
        
        // Verify job is no longer in DLQ
        List<JobData> dlqJobs = repo.getDLQJobs(10);
        for (JobData dlqJob : dlqJobs) {
            if (dlqJob.getId().equals(jobId)) {
                throw new AssertionError("Job should not be in DLQ after replay");
            }
        }
        
        db.close();
    }
    
    private static void testClearDLQ() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        // Clear DLQ first
        repo.clearDLQ();
        
        // Submit and move 5 jobs to DLQ
        for (int i = 0; i < 5; i++) {
            EmailJob job = new EmailJob("clear" + i + "@example.com", "Subject " + i, "Body " + i);
            repo.submitJob(job);
            String jobId = job.getId();
            repo.updateJobStatus(jobId, JobStatus.FAILED, "Error " + i);
            repo.moveToDeadLetterQueue(job, "Error " + i);
        }
        
        // Verify 5 jobs in DLQ
        List<JobData> dlqJobs = repo.getDLQJobs(10);
        if (dlqJobs.size() != 5) {
            throw new AssertionError("Expected 5 jobs in DLQ before clear, got " + dlqJobs.size());
        }
        
        // Clear DLQ
        int deleted = repo.clearDLQ();
        
        if (deleted != 5) {
            throw new AssertionError("Expected to delete 5 jobs, deleted " + deleted);
        }
        
        // Verify DLQ is empty
        dlqJobs = repo.getDLQJobs(10);
        if (!dlqJobs.isEmpty()) {
            throw new AssertionError("DLQ should be empty after clear, has " + dlqJobs.size() + " jobs");
        }
        
        db.close();
    }
    
    private static void testTransactionRollback() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        // Submit a job
        EmailJob job = new EmailJob("rollback@example.com", "Rollback Test", "Test Body");
        repo.submitJob(job);
        String jobId = job.getId();
        repo.updateJobStatus(jobId, JobStatus.FAILED, "Test failure");
        
        // Try to replay a non-existent job from DLQ
        boolean result = repo.replayFromDLQ("non-existent-job-id");
        
        if (result) {
            throw new AssertionError("Replaying non-existent job should return false");
        }
        
        // Verify original job is still in jobs table (not affected by failed replay)
        JobData jobData = repo.getJobById(jobId);
        if (jobData == null) {
            throw new AssertionError("Original job should still exist");
        }
        
        db.close();
    }
}

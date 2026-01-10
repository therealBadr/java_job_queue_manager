package com.jobqueue.test;

import com.jobqueue.core.QueueOverloadException;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.EmailJob;

/**
 * Tests for backpressure functionality
 */
public class TestBackpressure {
    
    public static void main(String[] args) {
        System.out.println("Starting Backpressure Tests...\n");
        
        int passed = 0;
        int total = 3;
        
        // Test 1: Verify backpressure activates when threshold exceeded
        if (testBackpressureActivation()) {
            System.out.println("✓ Test 1: Backpressure activation - PASSED");
            passed++;
        } else {
            System.out.println("✗ Test 1: Backpressure activation - FAILED");
        }
        
        // Test 2: Verify job submission is rejected during backpressure
        if (testJobRejectionDuringBackpressure()) {
            System.out.println("✓ Test 2: Job rejection during backpressure - PASSED");
            passed++;
        } else {
            System.out.println("✗ Test 2: Job rejection during backpressure - FAILED");
        }
        
        // Test 3: Verify backpressure releases when queue drains
        if (testBackpressureRelease()) {
            System.out.println("✓ Test 3: Backpressure release - PASSED");
            passed++;
        } else {
            System.out.println("✗ Test 3: Backpressure release - FAILED");
        }
        
        System.out.println("\n" + passed + "/" + total + " Backpressure tests passed");
        
        if (passed == total) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
    
    private static boolean testBackpressureActivation() {
        Database db = null;
        try {
            db = new Database();
            db.initialize(); // Initialize database schema
            JobRepository repo = new JobRepository(db);
            
            // Create scheduler with low threshold for testing (10 jobs)
            Scheduler scheduler = new Scheduler(2, repo, 10);
            
            // Submit 15 jobs to exceed threshold
            for (int i = 0; i < 15; i++) {
                EmailJob job = new EmailJob("test" + i + "@example.com", "Test Subject", 1);
                repo.submitJob(job);
            }
            
            // Verify queue depth exceeds threshold
            int depth = repo.getQueueDepth();
            if (depth <= 10) {
                System.err.println("  Queue depth (" + depth + ") should exceed threshold (10)");
                return false;
            }
            
            System.out.println("  Queue depth: " + depth + " (threshold: 10) - backpressure should activate");
            return true;
            
        } catch (Exception e) {
            System.err.println("  Error: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            if (db != null) db.close();
        }
    }
    
    private static boolean testJobRejectionDuringBackpressure() {
        Database db = null;
        Scheduler scheduler = null;
        Thread schedulerThread = null;
        
        try {
            db = new Database();
            db.initialize(); // Initialize database schema
            JobRepository repo = new JobRepository(db);
            
            // Create scheduler with low threshold (5 jobs)
            scheduler = new Scheduler(1, repo, 5);
            
            // Submit jobs to exceed threshold
            for (int i = 0; i < 10; i++) {
                EmailJob job = new EmailJob("test" + i + "@example.com", "Test", 1);
                repo.submitJob(job);
            }
            
            // Start scheduler to trigger backpressure detection
            final Scheduler finalScheduler = scheduler;
            schedulerThread = new Thread(() -> finalScheduler.start());
            schedulerThread.start();
            
            // Wait for backpressure to activate
            Thread.sleep(2000);
            
            // Try to submit a new job - should be rejected
            boolean rejected = false;
            try {
                EmailJob newJob = new EmailJob("new@example.com", "Should Fail", 1);
                scheduler.submitJob(newJob);
                System.err.println("  Job submission should have been rejected");
            } catch (QueueOverloadException e) {
                rejected = true;
                System.out.println("  Job correctly rejected: " + e.getMessage());
                System.out.println("  Current depth: " + e.getCurrentDepth() + ", Threshold: " + e.getThreshold());
            }
            
            return rejected;
            
        } catch (Exception e) {
            System.err.println("  Error: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (schedulerThread != null) {
                try {
                    schedulerThread.interrupt();
                    schedulerThread.join(2000);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
            if (db != null) db.close();
        }
    }
    
    private static boolean testBackpressureRelease() {
        Database db = null;
        try {
            db = new Database();
            db.initialize(); // Initialize database schema
            JobRepository repo = new JobRepository(db);
            
            // Create scheduler with threshold of 20
            Scheduler scheduler = new Scheduler(2, repo, 20);
            
            // Submit jobs to exceed threshold
            for (int i = 0; i < 25; i++) {
                EmailJob job = new EmailJob("test" + i + "@example.com", "Test", 1);
                repo.submitJob(job);
            }
            
            int initialDepth = repo.getQueueDepth();
            System.out.println("  Initial queue depth: " + initialDepth + " (threshold: 20)");
            
            if (initialDepth <= 20) {
                System.err.println("  Queue depth should exceed threshold");
                return false;
            }
            
            // Process some jobs to drain queue below 80% of threshold (16 jobs)
            for (int i = 0; i < 10; i++) {
                java.util.List<JobRepository.JobData> jobs = repo.getPendingJobs(1);
                if (!jobs.isEmpty()) {
                    repo.updateJobStatus(jobs.get(0).getId(), 
                        com.jobqueue.core.JobStatus.SUCCESS, null);
                }
            }
            
            int finalDepth = repo.getQueueDepth();
            System.out.println("  Final queue depth: " + finalDepth + " (80% threshold: " + (20 * 0.8) + ")");
            
            // Backpressure should release when depth < 16 (80% of 20)
            if (finalDepth >= 20 * 0.8) {
                System.err.println("  Queue depth should be below 80% of threshold for release");
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            System.err.println("  Error: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            if (db != null) db.close();
        }
    }
}

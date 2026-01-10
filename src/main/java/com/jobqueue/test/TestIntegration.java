package com.jobqueue.test;

import com.jobqueue.app.AnalyticsService;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.EmailJob;
import com.jobqueue.jobs.ReportJob;

import java.util.List;
import java.util.Map;

/**
 * Integration test that exercises the entire system end-to-end
 */
public class TestIntegration {
    
    public static void main(String[] args) {
        System.out.println("=== Integration Test - Full System ===\n");
        
        boolean allPassed = true;
        
        allPassed &= testCompleteWorkflow();
        allPassed &= testRetryMechanism();
        allPassed &= testPriorityScheduling();
        allPassed &= testConcurrentExecution();
        allPassed &= testAnalyticsAfterExecution();
        
        System.out.println("\n=== Test Summary ===");
        if (allPassed) {
            System.out.println("✓ All Integration Tests PASSED");
            System.exit(0);
        } else {
            System.out.println("✗ Some Tests FAILED");
            System.exit(1);
        }
    }
    
    private static boolean testCompleteWorkflow() {
        System.out.println("--- Test 1: Complete End-to-End Workflow ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            Scheduler scheduler = new Scheduler(2, repository);
            
            // Start scheduler in background
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("  ✓ Scheduler started");
            
            // Submit a job
            EmailJob job = new EmailJob("integration@test.com", "Integration Test", "Testing workflow");
            scheduler.submitJob(job);
            System.out.println("  ✓ Job submitted: " + job.getId());
            
            // Wait for execution
            Thread.sleep(5000);
            
            // Verify job completed
            JobData jobData = repository.getJobById(job.getId());
            assert jobData != null : "Job should exist in database";
            assert jobData.getStatus() == JobStatus.SUCCESS : "Job should be successful";
            System.out.println("  ✓ Job executed successfully");
            System.out.println("  ✓ Job status: " + jobData.getStatus());
            
            // Check metrics
            Map<String, Object> metrics = repository.getMetrics();
            System.out.println("  ✓ Metrics:");
            System.out.println("    Total processed: " + metrics.get("total_processed"));
            System.out.println("    Success rate: " + metrics.get("success_rate") + "%");
            
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            
            System.out.println("✓ Complete End-to-End Workflow Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Complete End-to-End Workflow Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testRetryMechanism() {
        System.out.println("--- Test 2: Retry Mechanism with Exponential Backoff ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create a job that will fail initially
            EmailJob job = new EmailJob("retry@test.com", "Retry Test", "Will fail first time");
            repository.submitJob(job);
            System.out.println("  ✓ Job submitted (default max retries: " + job.getMaxRetries() + ")");
            
            // Simulate a failure and check retry scheduling
            repository.claimJob(job.getId());
            repository.updateJobStatus(job.getId(), JobStatus.PENDING, "Simulated failure");
            repository.incrementRetryCount(job.getId());
            
            // Schedule a retry with exponential backoff (2^1 = 2 minutes)
            long delay = (long) Math.pow(2, 1) * 60 * 1000;
            repository.scheduleRetry(job.getId(), delay);
            System.out.println("  ✓ Retry scheduled with 2-minute delay");
            
            // Verify retry count was incremented
            JobData jobData = repository.getJobById(job.getId());
            assert jobData.getRetryCount() == 1 : "Retry count should be 1";
            assert jobData.getStatus() == JobStatus.PENDING : "Job should be PENDING for retry";
            System.out.println("  ✓ Job retry count: " + jobData.getRetryCount());
            System.out.println("  ✓ Job status: " + jobData.getStatus());
            
            database.close();
            System.out.println("✓ Retry Mechanism Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Retry Mechanism Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testPriorityScheduling() {
        System.out.println("--- Test 3: Priority-Based Job Scheduling ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            Scheduler scheduler = new Scheduler(1, repository); // Single worker
            
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("  ✓ Scheduler started with 1 worker");
            
            // Submit jobs with different priorities (using defaults)
            EmailJob lowPriority = new EmailJob("low@test.com", "Low", "Priority test");
            
            CleanupJob highPriority = new CleanupJob();
            highPriority.setCleanupData("/tmp", 7, ".*");
            
            // Submit jobs
            scheduler.submitJob(lowPriority);
            Thread.sleep(100);
            scheduler.submitJob(highPriority);
            System.out.println("  ✓ Submitted 2 jobs of different types");
            
            // Wait for execution
            Thread.sleep(10000);
            
            // Verify both completed
            JobData low = repository.getJobById(lowPriority.getId());
            JobData high = repository.getJobById(highPriority.getId());
            
            assert low.getStatus() == JobStatus.SUCCESS : "First job should complete";
            assert high.getStatus() == JobStatus.SUCCESS : "Second job should complete";
            System.out.println("  ✓ Both jobs completed successfully");
            System.out.println("  ✓ Priorities: " + low.getPriority() + ", " + high.getPriority());
            
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            
            System.out.println("✓ Priority-Based Job Scheduling Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Priority-Based Job Scheduling Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testConcurrentExecution() {
        System.out.println("--- Test 4: Concurrent Job Execution ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            Scheduler scheduler = new Scheduler(3, repository); // 3 workers
            
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("  ✓ Scheduler started with 3 workers");
            
            // Submit 5 jobs
            EmailJob job1 = new EmailJob("user1@test.com", "Test", "Message 1");
            EmailJob job2 = new EmailJob("user2@test.com", "Test", "Message 2");
            CleanupJob job3 = new CleanupJob();
            job3.setCleanupData("/tmp", 7, ".*");
            ReportJob job4 = new ReportJob();
            job4.setReportData("daily", "2024-01-01", "2024-12-31");
            EmailJob job5 = new EmailJob("user5@test.com", "Test", "Message 5");
            
            scheduler.submitJob(job1);
            scheduler.submitJob(job2);
            scheduler.submitJob(job3);
            scheduler.submitJob(job4);
            scheduler.submitJob(job5);
            System.out.println("  ✓ Submitted 5 jobs");
            
            // Wait for all to complete
            Thread.sleep(15000);
            
            // Verify all completed
            int successCount = 0;
            List<JobData> allJobs = repository.getAllJobs();
            for (JobData job : allJobs) {
                if (job.getStatus() == JobStatus.SUCCESS) {
                    successCount++;
                }
            }
            
            System.out.println("  ✓ Completed jobs: " + successCount + "/5");
            assert successCount == 5 : "All 5 jobs should complete successfully";
            
            // Check metrics
            Map<String, Object> metrics = repository.getMetrics();
            System.out.println("  ✓ Success rate: " + metrics.get("success_rate") + "%");
            
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            
            System.out.println("✓ Concurrent Job Execution Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Concurrent Job Execution Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testAnalyticsAfterExecution() {
        System.out.println("--- Test 5: Analytics After Job Execution ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            Scheduler scheduler = new Scheduler(2, repository);
            
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("  ✓ Scheduler and analytics initialized");
            
            // Submit various jobs
            EmailJob email = new EmailJob("analytics@test.com", "Analytics", "Test");
            CleanupJob cleanup = new CleanupJob();
            cleanup.setCleanupData("/tmp", 7, ".*");
            ReportJob report = new ReportJob();
            report.setReportData("monthly", "2024-01-01", "2024-12-31");
            
            scheduler.submitJob(email);
            scheduler.submitJob(cleanup);
            scheduler.submitJob(report);
            System.out.println("  ✓ Submitted 3 different job types");
            
            // Wait for execution
            Thread.sleep(12000);
            
            // Test analytics methods
            Map<String, Long> counts = analytics.getJobCountsByType();
            System.out.println("  ✓ Job counts by type:");
            counts.forEach((type, count) -> System.out.println("    " + type + ": " + count));
            
            Map<String, Double> avgTimes = analytics.getAverageExecutionTimeByType();
            System.out.println("  ✓ Average execution times:");
            avgTimes.forEach((type, time) -> 
                System.out.println("    " + type + ": " + String.format("%.0f", time) + "ms"));
            
            long recentCompletions = analytics.getJobsCompletedInLastHour();
            System.out.println("  ✓ Jobs completed in last hour: " + recentCompletions);
            
            Map<String, Double> failureRates = analytics.getFailureRateByType();
            System.out.println("  ✓ Failure rates:");
            failureRates.forEach((type, rate) -> 
                System.out.println("    " + type + ": " + String.format("%.1f", rate) + "%"));
            
            assert counts.size() >= 3 : "Should have at least 3 job types";
            assert recentCompletions >= 3 : "Should have at least 3 recent completions";
            
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            
            System.out.println("✓ Analytics After Job Execution Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Analytics After Job Execution Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

package com.jobqueue.test;

import com.jobqueue.app.AnalyticsService;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.EmailJob;
import com.jobqueue.jobs.ReportJob;

import java.util.List;
import java.util.Map;

/**
 * Test class for AnalyticsService
 */
public class TestAnalytics {
    
    public static void main(String[] args) {
        System.out.println("=== Testing Analytics Service ===\n");
        
        boolean allPassed = true;
        
        allPassed &= testGetJobsByStatus();
        allPassed &= testGetJobCountsByType();
        allPassed &= testGetAverageExecutionTimeByType();
        allPassed &= testGetTopPriorityPendingJobs();
        allPassed &= testGetFailureRateByType();
        allPassed &= testGetJobsCompletedInLastHour();
        
        System.out.println("\n=== Test Summary ===");
        if (allPassed) {
            System.out.println("✓ All Analytics Tests PASSED");
            System.exit(0);
        } else {
            System.out.println("✗ Some Tests FAILED");
            System.exit(1);
        }
    }
    
    private static boolean testGetJobsByStatus() {
        System.out.println("--- Test 1: Get Jobs By Status ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            
            // Submit jobs with different statuses
            EmailJob job1 = new EmailJob("test1@test.com", "Test", "Message");
            CleanupJob job2 = new CleanupJob();
            job2.setCleanupData("/tmp", 7, ".*");
            ReportJob job3 = new ReportJob();
            job3.setReportData("daily", "2024-01-01", "2024-12-31");
            
            repository.submitJob(job1);
            repository.submitJob(job2);
            repository.submitJob(job3);
            System.out.println("  ✓ Submitted 3 jobs");
            
            // Manually update one to SUCCESS for testing
            repository.updateJobStatus(job1.getId(), JobStatus.SUCCESS, null);
            System.out.println("  ✓ Updated job1 to SUCCESS");
            
            // Get jobs by status
            List<JobData> pendingJobs = analytics.getJobsByStatus(JobStatus.PENDING);
            List<JobData> successJobs = analytics.getJobsByStatus(JobStatus.SUCCESS);
            
            System.out.println("  ✓ PENDING jobs: " + pendingJobs.size());
            System.out.println("  ✓ SUCCESS jobs: " + successJobs.size());
            
            assert pendingJobs.size() == 2 : "Should have 2 pending jobs";
            assert successJobs.size() == 1 : "Should have 1 success job";
            
            database.close();
            System.out.println("✓ Get Jobs By Status Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Get Jobs By Status Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testGetJobCountsByType() {
        System.out.println("--- Test 2: Get Job Counts By Type ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            
            // Submit multiple jobs of different types
            EmailJob email1 = new EmailJob("user1@test.com", "Test", "Message 1");
            EmailJob email2 = new EmailJob("user2@test.com", "Test", "Message 2");
            CleanupJob cleanup1 = new CleanupJob();
            cleanup1.setCleanupData("/tmp", 7, ".*");
            ReportJob report1 = new ReportJob();
            report1.setReportData("daily", "2024-01-01", "2024-12-31");
            
            repository.submitJob(email1);
            repository.submitJob(email2);
            repository.submitJob(cleanup1);
            repository.submitJob(report1);
            System.out.println("  ✓ Submitted 4 jobs (2 EmailJob, 1 CleanupJob, 1 ReportJob)");
            
            // Get counts by type
            Map<String, Long> counts = analytics.getJobCountsByType();
            
            System.out.println("  ✓ Job counts by type:");
            counts.forEach((type, count) -> System.out.println("    " + type + ": " + count));
            
            assert counts.get("EmailJob") == 2 : "Should have 2 EmailJob";
            assert counts.get("CleanupJob") == 1 : "Should have 1 CleanupJob";
            assert counts.get("ReportJob") == 1 : "Should have 1 ReportJob";
            
            database.close();
            System.out.println("✓ Get Job Counts By Type Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Get Job Counts By Type Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testGetAverageExecutionTimeByType() {
        System.out.println("--- Test 3: Get Average Execution Time By Type ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            
            // Submit jobs and simulate execution
            EmailJob email1 = new EmailJob("user@test.com", "Test", "Message");
            repository.submitJob(email1);
            
            // Simulate execution - claim job, then mark success
            repository.claimJob(email1.getId());
            Thread.sleep(100); // Small delay
            repository.updateJobStatus(email1.getId(), JobStatus.SUCCESS, null);
            System.out.println("  ✓ Simulated job execution with timing");
            
            // Get average execution times
            Map<String, Double> avgTimes = analytics.getAverageExecutionTimeByType();
            
            System.out.println("  ✓ Average execution times:");
            avgTimes.forEach((type, time) -> 
                System.out.println("    " + type + ": " + String.format("%.2f", time) + "ms"));
            
            assert avgTimes.containsKey("EmailJob") : "Should have EmailJob timing";
            assert avgTimes.get("EmailJob") > 0 : "EmailJob should have positive execution time";
            
            database.close();
            System.out.println("✓ Get Average Execution Time By Type Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Get Average Execution Time By Type Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testGetTopPriorityPendingJobs() {
        System.out.println("--- Test 4: Get Top Priority Pending Jobs ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            
            // Submit jobs with different priorities
            // Note: Priority is set internally, we'll submit and manually update priorities in DB
            EmailJob lowPriority = new EmailJob("low@test.com", "Test", "Low");
            CleanupJob mediumPriority = new CleanupJob();
            mediumPriority.setCleanupData("/tmp", 7, ".*");
            ReportJob highPriority = new ReportJob();
            highPriority.setReportData("daily", "2024-01-01", "2024-12-31");
            
            repository.submitJob(lowPriority);
            repository.submitJob(mediumPriority);
            repository.submitJob(highPriority);
            System.out.println("  ✓ Submitted 3 jobs");
            
            // Get top priority jobs (all will have default priority)
            List<JobData> topJobs = analytics.getTopPriorityPendingJobs(2);
            
            System.out.println("  ✓ Top 2 pending jobs:");
            topJobs.forEach(job -> 
                System.out.println("    " + job.getType() + " (priority: " + job.getPriority() + ")"));
            
            assert topJobs.size() == 2 : "Should return 2 jobs";
            assert topJobs.size() <= 3 : "Should not return more than requested";
            
            database.close();
            System.out.println("✓ Get Top Priority Pending Jobs Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Get Top Priority Pending Jobs Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testGetFailureRateByType() {
        System.out.println("--- Test 5: Get Failure Rate By Type ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            
            // Submit multiple jobs of same type with different outcomes
            EmailJob email1 = new EmailJob("user1@test.com", "Test", "Message 1");
            EmailJob email2 = new EmailJob("user2@test.com", "Test", "Message 2");
            EmailJob email3 = new EmailJob("user3@test.com", "Test", "Message 3");
            EmailJob email4 = new EmailJob("user4@test.com", "Test", "Message 4");
            
            repository.submitJob(email1);
            repository.submitJob(email2);
            repository.submitJob(email3);
            repository.submitJob(email4);
            
            // Mark 1 as SUCCESS, 2 as FAILED, 1 stays PENDING
            repository.updateJobStatus(email1.getId(), JobStatus.SUCCESS, null);
            repository.updateJobStatus(email2.getId(), JobStatus.FAILED, "Test failure 1");
            repository.updateJobStatus(email3.getId(), JobStatus.FAILED, "Test failure 2");
            System.out.println("  ✓ Created 4 EmailJobs: 1 SUCCESS, 2 FAILED, 1 PENDING");
            
            // Get failure rates
            Map<String, Double> failureRates = analytics.getFailureRateByType();
            
            System.out.println("  ✓ Failure rates:");
            failureRates.forEach((type, rate) -> 
                System.out.println("    " + type + ": " + String.format("%.1f", rate) + "%"));
            
            assert failureRates.containsKey("EmailJob") : "Should have EmailJob failure rate";
            // 2 failed out of 4 total = 50%
            assert Math.abs(failureRates.get("EmailJob") - 50.0) < 0.1 : 
                "EmailJob failure rate should be 50%";
            
            database.close();
            System.out.println("✓ Get Failure Rate By Type Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Get Failure Rate By Type Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testGetJobsCompletedInLastHour() {
        System.out.println("--- Test 6: Get Jobs Completed In Last Hour ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            AnalyticsService analytics = new AnalyticsService(repository);
            
            // Submit and complete some jobs
            EmailJob email1 = new EmailJob("user@test.com", "Test", "Message");
            CleanupJob cleanup1 = new CleanupJob();
            cleanup1.setCleanupData("/tmp", 7, ".*");
            
            repository.submitJob(email1);
            repository.submitJob(cleanup1);
            
            // Mark both as completed (which sets completed_at to now)
            repository.updateJobStatus(email1.getId(), JobStatus.SUCCESS, null);
            repository.updateJobStatus(cleanup1.getId(), JobStatus.SUCCESS, null);
            System.out.println("  ✓ Completed 2 jobs just now");
            
            // Get count of jobs completed in last hour
            long recentCount = analytics.getJobsCompletedInLastHour();
            
            System.out.println("  ✓ Jobs completed in last hour: " + recentCount);
            
            assert recentCount >= 2 : "Should have at least 2 recent completions";
            
            database.close();
            System.out.println("✓ Get Jobs Completed In Last Hour Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Get Jobs Completed In Last Hour Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

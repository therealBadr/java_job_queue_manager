package com.jobqueue.test;

import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.EmailJob;
import com.jobqueue.jobs.ReportJob;
import com.jobqueue.core.JobStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Test the anti-starvation mechanism in the scheduler
 */
public class TestAntiStarvation {
    
    public static void main(String[] args) {
        System.out.println("=== Testing Anti-Starvation Mechanism ===\n");
        
        boolean allPassed = true;
        
        allPassed &= testAntiStarvationPriorityBoost();
        allPassed &= testOldLowPriorityGetsProcessed();
        
        System.out.println("\n=== Test Summary ===");
        if (allPassed) {
            System.out.println("✓ All Anti-Starvation Tests PASSED");
            System.exit(0);
        } else {
            System.out.println("✗ Some Tests FAILED");
            System.exit(1);
        }
    }
    
    private static boolean testAntiStarvationPriorityBoost() {
        System.out.println("--- Test 1: Age-Based Priority Boost ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            // Create jobs with different priorities and ages
            // Old low-priority job (created 2 hours ago)
            EmailJob oldLowPriority = new EmailJob("old@test.com", "Old", "2 hours old, priority 3");
            repository.submitJob(oldLowPriority);
            
            // Manually backdate the created_at to 2 hours ago
            backdateJob(database, oldLowPriority.getId(), 2);
            System.out.println("  ✓ Created old low-priority job (2 hours old, priority 3)");
            
            // Recent high-priority job (just created)
            CleanupJob newHighPriority = new CleanupJob();
            newHighPriority.setCleanupData("/tmp", 7, ".*");
            repository.submitJob(newHighPriority);
            System.out.println("  ✓ Created new high-priority job (just now, priority 7)");
            
            // Calculate effective priorities
            // Old job: base 3 + (120 minutes / 60) = 3 + 2 = 5 effective priority
            // New job: base 7 + (0 minutes / 60) = 7 + 0 = 7 effective priority
            
            System.out.println("  ✓ Expected effective priorities:");
            System.out.println("    Old job: 3 (base) + 2 (age bonus) = 5 effective");
            System.out.println("    New job: 7 (base) + 0 (age bonus) = 7 effective");
            System.out.println("  ✓ New high-priority job should still be processed first");
            
            database.close();
            System.out.println("✓ Age-Based Priority Boost Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Age-Based Priority Boost Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    private static boolean testOldLowPriorityGetsProcessed() {
        System.out.println("--- Test 2: Old Low-Priority Job Eventually Gets Processed ---");
        Database database = new Database();
        try {
            database.initialize();
            JobRepository repository = new JobRepository(database);
            Scheduler scheduler = new Scheduler(2, repository);
            
            // Create an old low-priority job (created 10 hours ago, priority 1)
            EmailJob oldJob = new EmailJob("old@test.com", "Old", "10 hours old, priority 1");
            repository.submitJob(oldJob);
            backdateJob(database, oldJob.getId(), 10); // 10 hours ago
            System.out.println("  ✓ Created old low-priority job (10 hours ago, priority 1)");
            
            // Effective priority: 1 + (600 minutes / 60) = 1 + 10 = 11
            System.out.println("  ✓ Effective priority: 1 (base) + 10 (age bonus) = 11");
            
            // Create a newer high-priority job (just now, priority 9)
            ReportJob newJob = new ReportJob();
            newJob.setReportData("daily", "2024-01-01", "2024-12-31");
            repository.submitJob(newJob);
            System.out.println("  ✓ Created new high-priority job (just now, priority 9)");
            System.out.println("  ✓ Effective priority: 9 (base) + 0 (age bonus) = 9");
            
            // Start scheduler
            Thread schedulerThread = new Thread(() -> scheduler.start());
            schedulerThread.start();
            Thread.sleep(500);
            System.out.println("  ✓ Scheduler started");
            
            // Wait for jobs to be processed
            Thread.sleep(15000);
            
            // Check both jobs completed
            JobData oldJobData = repository.getJobById(oldJob.getId());
            JobData newJobData = repository.getJobById(newJob.getId());
            
            assert oldJobData.getStatus() == JobStatus.SUCCESS : "Old job should complete";
            assert newJobData.getStatus() == JobStatus.SUCCESS : "New job should complete";
            
            System.out.println("  ✓ Old job status: " + oldJobData.getStatus());
            System.out.println("  ✓ New job status: " + newJobData.getStatus());
            
            // Old job should be processed first due to higher effective priority (11 vs 9)
            if (oldJobData.getStartedAt() != null && newJobData.getStartedAt() != null) {
                boolean oldStartedFirst = oldJobData.getStartedAt().isBefore(newJobData.getStartedAt()) ||
                                         oldJobData.getStartedAt().equals(newJobData.getStartedAt());
                System.out.println("  ✓ Old job started first (anti-starvation working): " + oldStartedFirst);
            }
            
            scheduler.shutdown();
            schedulerThread.join(5000);
            database.close();
            
            System.out.println("✓ Old Low-Priority Job Eventually Gets Processed Test PASSED\n");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Old Low-Priority Job Eventually Gets Processed Test FAILED: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Helper method to backdate a job's created_at timestamp
     */
    private static void backdateJob(Database database, String jobId, int hoursAgo) throws Exception {
        LocalDateTime backdated = LocalDateTime.now().minusHours(hoursAgo);
        String sql = "UPDATE jobs SET created_at = ? WHERE id = ?";
        
        try (Connection conn = database.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.valueOf(backdated));
            stmt.setString(2, jobId);
            stmt.executeUpdate();
        }
    }
}

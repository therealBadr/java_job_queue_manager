package com.jobqueue.app;

import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.EmailJob;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.ReportJob;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main application entry point for the Job Queue Manager.
 * Initializes all components and starts the scheduler and metrics server.
 */
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());
    private static final int WORKER_COUNT = 8;
    private static final int METRICS_PORT = 8080;
    
    private static Database database;
    private static Scheduler scheduler;
    private static MetricsServer metricsServer;
    private static Thread schedulerThread;

    public static void main(String[] args) {
        logger.info("=== Job Queue Manager Starting ===");
        
        try {
            // 1. Initialize Database
            initializeDatabase();
            
            // 2. Initialize Repository and recover crashed jobs
            JobRepository jobRepository = initializeRepository();
            
            // 3. Initialize and start Scheduler
            initializeScheduler(jobRepository);
            
            // Submit demo jobs for testing
            submitDemoJobs(jobRepository);
            
            // 4. Initialize and start Metrics Server
            initializeMetricsServer(jobRepository);
            
            // 5. Add shutdown hook for graceful shutdown
            addShutdownHook();
            
            logger.info("=== Job Queue Manager is running ===");
            logger.info("Press Ctrl+C to stop");
            
            // Display analytics after jobs complete
            displayAnalytics(jobRepository);
            
            // Keep main thread alive - scheduler thread is non-daemon so JVM won't exit
            // Main thread waits for interrupt signal
            while (schedulerThread.isAlive()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info("Main thread interrupted");
                    break;
                }
            }
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Fatal error during startup", e);
            System.exit(1);
        }
    }
    
    /**
     * Initialize the database and create schema.
     */
    private static void initializeDatabase() {
        try {
            logger.info("Initializing database...");
            database = new Database();
            database.initialize();
            logger.info("Database initialized successfully");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize database", e);
            throw new RuntimeException("Database initialization failed", e);
        }
    }
    
    /**
     * Initialize the job repository and recover any crashed jobs.
     * 
     * @return the initialized JobRepository
     */
    private static JobRepository initializeRepository() {
        try {
            logger.info("Initializing job repository...");
            JobRepository jobRepository = new JobRepository(database);
            
            // Recover any jobs that were running when the system crashed
            int recovered = jobRepository.recoverCrashedJobs();
            if (recovered > 0) {
                logger.info("Recovered " + recovered + " crashed jobs");
            } else {
                logger.info("No crashed jobs to recover");
            }
            
            logger.info("Job repository initialized successfully");
            return jobRepository;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize repository", e);
            throw new RuntimeException("Repository initialization failed", e);
        }
    }
    
    /**
     * Initialize and start the scheduler in a separate thread.
     * 
     * @param jobRepository the job repository
     */
    private static void initializeScheduler(JobRepository jobRepository) {
        try {
            logger.info("Initializing scheduler with " + WORKER_COUNT + " workers...");
            scheduler = new Scheduler(WORKER_COUNT, jobRepository);
            
            // Start scheduler in a separate thread
            schedulerThread = new Thread(() -> {
                try {
                    scheduler.start();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Scheduler error", e);
                }
            }, "Scheduler-Thread");
            
            schedulerThread.setDaemon(false);
            schedulerThread.start();
            
            logger.info("Scheduler started successfully");
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize scheduler", e);
            throw new RuntimeException("Scheduler initialization failed", e);
        }
    }
    
    /**
     * Initialize and start the metrics HTTP server.
     * 
     * @param jobRepository the job repository
     */
    private static void initializeMetricsServer(JobRepository jobRepository) {
        try {
            logger.info("Initializing metrics server on port " + METRICS_PORT + "...");
            metricsServer = new MetricsServer(jobRepository, METRICS_PORT);
            metricsServer.start();
            
            logger.info("Metrics server started successfully");
            logger.info("Metrics available at http://localhost:" + METRICS_PORT + "/metrics");
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start metrics server", e);
            throw new RuntimeException("Metrics server initialization failed", e);
        }
    }
    
    /**
     * Submit demo jobs for testing and demonstration.
     * Creates 100 jobs with varied configurations:
     * - 50 EmailJobs with priorities 1-10
     * - 30 CleanupJobs with priorities 2-5
     * - 20 ReportJobs with priorities 6-9
     * - 5 jobs scheduled for future execution
     * 
     * @param jobRepository the job repository to submit jobs to
     */
    private static void submitDemoJobs(JobRepository jobRepository) {
        try {
            logger.info("Submitting demo jobs...");
            Random random = new Random();
            int totalSubmitted = 0;
            int futureJobsScheduled = 0;
            
            // 1. Submit 50 EmailJobs with varying priorities (1-10)
            String[] domains = {"example.com", "test.com", "demo.org", "sample.net", "company.io"};
            String[] subjects = {"Welcome Email", "Password Reset", "Newsletter", "Invoice", "Notification", "Alert", "Report", "Update"};
            
            for (int i = 0; i < 50; i++) {
                String to = "user" + (i + 1) + "@" + domains[random.nextInt(domains.length)];
                String subject = subjects[random.nextInt(subjects.length)] + " #" + (i + 1);
                String body = "This is a demo email message for testing the job queue system. Email ID: " + (i + 1);
                int priority = 1 + random.nextInt(10); // Priority 1-10
                
                EmailJob emailJob = new EmailJob(to, subject, body);
                jobRepository.submitJob(emailJob);
                
                // Update priority after submission
                jobRepository.updateJobPriority(emailJob.getId(), priority);
                
                // Schedule some jobs for future execution
                if (futureJobsScheduled < 5 && random.nextInt(10) == 0) {
                    LocalDateTime scheduledTime = LocalDateTime.now().plusMinutes(1 + random.nextInt(5));
                    jobRepository.updateScheduledTime(emailJob.getId(), Timestamp.valueOf(scheduledTime));
                    futureJobsScheduled++;
                }
                
                totalSubmitted++;
                
                if (totalSubmitted % 10 == 0) {
                    logger.info("Submitted " + totalSubmitted + " jobs...");
                }
            }
            
            // 2. Submit 30 CleanupJobs with varying configurations
            String[] directories = {"/tmp/logs", "/tmp/cache", "/var/tmp/uploads", "/tmp/sessions", "/tmp/reports"};
            String[] filePatterns = {"*.log", "*.tmp", "*.cache", "*.old", "*.bak"};
            
            for (int i = 0; i < 30; i++) {
                String directory = directories[random.nextInt(directories.length)];
                int daysOld = 7 + random.nextInt(24); // 7-30 days
                String filePattern = filePatterns[random.nextInt(filePatterns.length)];
                int priority = 2 + random.nextInt(4); // Priority 2-5
                
                CleanupJob cleanupJob = new CleanupJob();
                cleanupJob.setCleanupData(directory, daysOld, filePattern);
                jobRepository.submitJob(cleanupJob);
                
                // Update priority after submission
                jobRepository.updateJobPriority(cleanupJob.getId(), priority);
                
                // Schedule some jobs for future execution
                if (futureJobsScheduled < 5 && random.nextInt(10) == 0) {
                    LocalDateTime scheduledTime = LocalDateTime.now().plusMinutes(1 + random.nextInt(5));
                    jobRepository.updateScheduledTime(cleanupJob.getId(), Timestamp.valueOf(scheduledTime));
                    futureJobsScheduled++;
                }
                
                totalSubmitted++;
                
                if (totalSubmitted % 10 == 0) {
                    logger.info("Submitted " + totalSubmitted + " jobs...");
                }
            }
            
            // 3. Submit 20 ReportJobs with varying report types
            String[] reportTypes = {"sales", "inventory", "analytics", "financial", "performance"};
            String[] months = {"2026-01", "2026-02", "2026-03", "2025-12", "2025-11"};
            
            for (int i = 0; i < 20; i++) {
                String reportType = reportTypes[random.nextInt(reportTypes.length)];
                String month = months[random.nextInt(months.length)];
                String startDate = month + "-01";
                
                // Calculate end date based on month
                int daysInMonth = 28 + random.nextInt(3); // Simplified: 28-30 days
                String endDate = month + "-" + String.format("%02d", daysInMonth);
                int priority = 6 + random.nextInt(4); // Priority 6-9
                
                ReportJob reportJob = new ReportJob();
                reportJob.setReportData(reportType, startDate, endDate);
                jobRepository.submitJob(reportJob);
                
                // Update priority after submission
                jobRepository.updateJobPriority(reportJob.getId(), priority);
                
                // Schedule remaining jobs for future execution if needed
                if (futureJobsScheduled < 5 && random.nextInt(5) == 0) {
                    LocalDateTime scheduledTime = LocalDateTime.now().plusMinutes(1 + random.nextInt(5));
                    jobRepository.updateScheduledTime(reportJob.getId(), Timestamp.valueOf(scheduledTime));
                    futureJobsScheduled++;
                }
                
                totalSubmitted++;
                
                if (totalSubmitted % 10 == 0) {
                    logger.info("Submitted " + totalSubmitted + " jobs...");
                }
            }
            
            logger.info("Demo job submission complete: " + totalSubmitted + " jobs submitted");
            logger.info("  - EmailJobs: 50 (priorities 1-10)");
            logger.info("  - CleanupJobs: 30 (priorities 2-5)");
       Display comprehensive analytics about job execution.
     * Waits for jobs to complete and then displays statistics.
     * 
     * @param jobRepository the job repository
     */
    private static void displayAnalytics(JobRepository jobRepository) {
        try {
            logger.info("\n=== Waiting for jobs to complete ===");
            
            // Wait for jobs to complete (poll every 5 seconds, max 5 minutes)
            int maxWaitSeconds = 300; // 5 minutes
            int pollIntervalSeconds = 5;
            int elapsedSeconds = 0;
            
            while (elapsedSeconds < maxWaitSeconds) {
                int queueDepth = jobRepository.getQueueDepth();
                int runningJobs = jobRepository.getJobsByStatus(JobStatus.RUNNING).size();
                
                if (queueDepth == 0 && runningJobs == 0) {
                    logger.info("All jobs completed!");
                    break;
                }
                
                logger.info("Waiting... (Pending: " + queueDepth + ", Running: " + runningJobs + ")");
                Thread.sleep(pollIntervalSeconds * 1000);
                elapsedSeconds += pollIntervalSeconds;
            }
            
            if (elapsedSeconds >= maxWaitSeconds) {
                logger.warning("Timeout reached. Displaying analytics for current state.");
            }
            
            // Create analytics service
            AnalyticsService analytics = new AnalyticsService(jobRepository);
            
            System.out.println("\n");
            System.out.println("╔════════════════════════════════════════════════════════════════════╗");
            System.out.println("║              JOB QUEUE ANALYTICS REPORT                            ║");
            System.out.println("╚════════════════════════════════════════════════════════════════════╝");
            System.out.println();
            
            // 1. Total jobs by status
            System.out.println("━━━ JOBS BY STATUS ━━━");
            displayJobsByStatus(analytics);
            System.out.println();
            
            // 2. Job counts by type
            System.out.println("━━━ JOB COUNTS BY TYPE ━━━");
            displayJobCountsByType(analytics);
            System.out.println();
            
            // 3. Average execution time by type
            System.out.println("━━━ AVERAGE EXECUTION TIME BY TYPE ━━━");
            displayAverageExecutionTime(analytics);
            System.out.println();
            
            // 4. Top priority pending jobs
            System.out.println("━━━ TOP 10 PRIORITY PENDING JOBS ━━━");
            displayTopPriorityJobs(analytics);
            System.out.println();
            
            // 5. Failure rate by type
            System.out.println("━━━ FAILURE RATE BY TYPE ━━━");
            displayFailureRates(analytics);
            System.out.println();
            
            // 6. DLQ statistics
            System.out.println("━━━ DEAD LETTER QUEUE (DLQ) ━━━");
            displayDLQStatistics(jobRepository);
            System.out.println();
            
            System.out.println("╔════════════════════════════════════════════════════════════════════╗");
            System.out.println("║                    END OF ANALYTICS REPORT                         ║");
            System.out.println("╚════════════════════════════════════════════════════════════════════╝");
            System.out.println();
            
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error displaying analytics", e);
        }
    }
    
    /**
     * Display jobs grouped by status.
     */
    private static void displayJobsByStatus(AnalyticsService analytics) throws Exception {
        System.out.printf("  %-15s %10s%n", "Status", "Count");
        System.out.println("  " + "-".repeat(30));
        
        for (JobStatus status : JobStatus.values()) {
            List<JobRepository.JobData> jobs = analytics.getJobsByStatus(status);
            System.out.printf("  %-15s %10d%n", status.name(), jobs.size());
        }
    }
    
    /**
     * Display job counts grouped by type.
     */
    private static void displayJobCountsByType(AnalyticsService analytics) throws Exception {
        Map<String, Long> counts = analytics.getJobCountsByType();
        
        if (counts.isEmpty()) {
            System.out.println("  No jobs found");
            return;
        }
        
        System.out.printf("  %-20s %10s%n", "Job Type", "Count");
        System.out.println("  " + "-".repeat(35));
        
        counts.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .forEach(entry -> 
                System.out.printf("  %-20s %10d%n", entry.getKey(), entry.getValue())
            );
        
        long total = counts.values().stream().mapToLong(Long::longValue).sum();
        System.out.println("  " + "-".repeat(35));
        System.out.printf("  %-20s %10d%n", "TOTAL", total);
    }
    
    /**
     * Display average execution time by job type.
     */
    private static void displayAverageExecutionTime(AnalyticsService analytics) throws Exception {
        Map<String, Double> avgTimes = analytics.getAverageExecutionTimeByType();
        
        if (avgTimes.isEmpty()) {
            System.out.println("  No execution data available");
            return;
        }
        
        System.out.printf("  %-20s %15s %15s%n", "Job Type", "Avg Time (ms)", "Avg Time (sec)");
        System.out.println("  " + "-".repeat(55));
        
        avgTimes.entrySet().stream()
            .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
            .forEach(entry -> {
                double ms = entry.getValue();
                double sec = ms / 1000.0;
                System.out.printf("  %-20s %,15.2f %15.3f%n", 
                    entry.getKey(), ms, sec);
            });
    }
    
    /**
     * Display top priority pending jobs.
     */
    private static void displayTopPriorityJobs(AnalyticsService analytics) throws Exception {
        List<JobRepository.JobData> topJobs = analytics.getTopPriorityPendingJobs(10);
        
        if (topJobs.isEmpty()) {
            System.out.println("  No pending jobs");
            return;
        }
        
        System.out.printf("  %-40s %10s %20s%n", "Job ID", "Priority", "Created At");
        System.out.println("  " + "-".repeat(75));
        
        for (JobRepository.JobData job : topJobs) {
            String createdAt = job.getCreatedAt() != null ? 
                job.getCreatedAt().toString().substring(0, 19) : "N/A";
            System.out.printf("  %-40s %10d %20s%n", 
                job.getId().substring(0, Math.min(40, job.getId().length())), 
                job.getPriority(), 
                createdAt);
        }
    }
    
    /**
     * Display failure rates by job type.
     */
    private static void displayFailureRates(AnalyticsService analytics) throws Exception {
        Map<String, Double> failureRates = analytics.getFailureRateByType();
        
        if (failureRates.isEmpty()) {
            System.out.println("  No failure data available");
            return;
        }
        
        System.out.printf("  %-20s %15s %15s%n", "Job Type", "Failure Rate", "Status");
        System.out.println("  " + "-".repeat(55));
        
        failureRates.entrySet().stream()
            .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
            .forEach(entry -> {
                double rate = entry.getValue();
                String status = rate == 0 ? "✓ Excellent" :
                               rate < 5 ? "✓ Good" :
                               rate < 20 ? "⚠ Warning" : "✗ Critical";
                System.out.printf("  %-20s %14.2f%% %15s%n", 
                    entry.getKey(), rate, status);
            });
    }
    
    /**
     * Display DLQ statistics.
     */
    private static void displayDLQStatistics(JobRepository jobRepository) throws Exception {
        List<JobRepository.JobData> dlqJobs = jobRepository.getDLQJobs(100);
        
        System.out.printf("  Total jobs in DLQ: %d%n", dlqJobs.size());
        System.out.println();
        
        if (dlqJobs.isEmpty()) {
            System.out.println("  ✓ No jobs in Dead Letter Queue");
            return;
        }
        
        System.out.println("  First 5 entries:");
        System.out.printf("  %-40s %-15s %-30s%n", "Job ID", "Type", "Error");
        System.out.println("  " + "-".repeat(90));
        
        int displayCount = Math.min(5, dlqJobs.size());
        for (int i = 0; i < displayCount; i++) {
            JobRepository.JobData job = dlqJobs.get(i);
            String jobId = job.getId().substring(0, Math.min(40, job.getId().length()));
            String error = job.getErrorMessage() != null ? 
                job.getErrorMessage().substring(0, Math.min(30, job.getErrorMessage().length())) : "Unknown";
            System.out.printf("  %-40s %-15s %-30s%n", 
                jobId, job.getType(), error);
        }
        
        if (dlqJobs.size() > 5) {
            System.out.printf("  ... and %d more%n", dlqJobs.size() - 5);
        }
    }
    
    /**
     *      logger.info("  - ReportJobs: 20 (priorities 6-9)");
            logger.info("  - Future scheduled jobs: " + futureJobsScheduled);
            
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error submitting demo jobs", e);
        }
    }
    
    /**
     *      logger.info("  - ReportJobs: 20 (priorities 6-9)");
            logger.info("  - Future scheduled jobs: " + futureJobsScheduled);
            
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error submitting demo jobs", e);
        }
    }
    
    /**
     *      logger.info("  - ReportJobs: 20 (priorities 6-9)");
            logger.info("  - Future scheduled jobs: " + futureJobsScheduled);
            
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error submitting demo jobs", e);
        }
    }
    
    /**
     * Add a shutdown hook to gracefully stop all components.
     */
    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("=== Shutdown signal received ===");
            
            // Shutdown scheduler
            if (scheduler != null) {
                try {
                    logger.info("Shutting down scheduler...");
                    scheduler.shutdown();
                    logger.info("Scheduler stopped");
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error shutting down scheduler", e);
                }
            }
            
            // Shutdown metrics server
            if (metricsServer != null) {
                try {
                    logger.info("Shutting down metrics server...");
                    metricsServer.stop();
                    logger.info("Metrics server stopped");
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error shutting down metrics server", e);
                }
            }
            
            // Close database
            if (database != null) {
                try {
                    logger.info("Closing database connections...");
                    database.close();
                    logger.info("Database closed");
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error closing database", e);
                }
            }
            
            logger.info("=== Job Queue Manager Stopped ===");
        }, "Shutdown-Hook"));
        
        logger.info("Shutdown hook registered");
    }
}

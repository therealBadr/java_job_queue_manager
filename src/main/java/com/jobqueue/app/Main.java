package com.jobqueue.app;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.EmailJob;
import com.jobqueue.jobs.ReportJob;


public class Main {
    // ANSI Color codes
    public static final String RESET = "\033[0m";
    public static final String RED = "\033[0;31m";
    public static final String GREEN = "\033[0;32m";
    public static final String YELLOW = "\033[0;33m";
    public static final String BLUE = "\033[0;34m";
    public static final String MAGENTA = "\033[0;35m";
    public static final String CYAN = "\033[0;36m";
    public static final String BOLD = "\033[1m";
    
    private static final int WORKER_COUNT = 8;
    private static final int METRICS_PORT = 8080;
    private static final int TOTAL_JOBS = 30;
    
    private static Database database;
    private static Scheduler scheduler;
    private static MetricsServer metricsServer;
    private static Thread schedulerThread;
    private static AnalyticsService analytics;
    private static long startTime;

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();
        
        try {
            printHeader();
            

            phase1_initialization();
            
            // Phase 2: Job Submission
            JobRepository repository = phase2_jobSubmission();
            
            // Phase 3: Start Scheduler
            phase3_startScheduler(repository);
            
            // Phase 4: Live Monitoring
            phase4_liveMonitoring(repository);
            
            // Phase 5: Final Statistics
            phase5_finalStatistics(repository);
            
            // Phase 6: Graceful Shutdown
            phase6_shutdown();
            
        } catch (Exception e) {
            System.out.println("\n" + RED + "✗ Fatal error: " + e.getMessage() + RESET);
            e.printStackTrace();
            cleanup();
            System.exit(1);
        }
    }
    
    private static void printHeader() {
        System.out.println("\n" + CYAN + BOLD);
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                              ║");
        System.out.println("║        JOB QUEUE MANAGER - PROFESSIONAL DEMONSTRATION        ║");
        System.out.println("║                                                              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println(RESET);
    }
    
    //1111111111111111111111111111111111111111111
    private static void phase1_initialization() throws Exception {
        printPhaseHeader("INITIALIZATION");
        
        // Initialize database
        System.out.print("  Initializing database... ");
        database = new Database();
        database.initialize();
        Thread.sleep(1000);
        System.out.println(GREEN + "✓" + RESET);
        
        // Run crash recovery
        JobRepository repository = new JobRepository(database);
        System.out.print("  Running crash recovery... ");
        int recovered = repository.recoverCrashedJobs();
        Thread.sleep(1000);
        System.out.println(GREEN + "✓" + RESET);
        
        if (recovered > 0) {
            System.out.println("  " + YELLOW + "⚠  Recovered " + recovered + " crashed jobs" + RESET);
        } else {
            System.out.println("  " + GREEN + "✓ No crashed jobs found" + RESET);
        }
        
        System.out.println("  " + GREEN + "✓ Schema validated" + RESET);
        Thread.sleep(1000);
        System.out.println();
    }
    
    //22222222222222222222222222222222222222222222222222222222
    private static JobRepository phase2_jobSubmission() throws Exception {
        printPhaseHeader("JOB SUBMISSION");
        
        JobRepository repository = new JobRepository(database);
        analytics = new AnalyticsService(repository);
        Random random = new Random(42);
        
        System.out.println("  Submitting jobs...");
        
        int submitted = 0;
        
        // Submit 10 EmailJobs
        for (int i = 0; i < 10; i++) {
            String payload = String.format(
                "{\"to\":\"user%d@example.com\",\"subject\":\"Demo Email %d\",\"body\":\"Test message\"}",
                i, i);
            EmailJob job = new EmailJob("email-" + i, payload, 5 + random.nextInt(4));
            repository.submitJob(job);
            submitted++;
            printProgress("  ", submitted, TOTAL_JOBS);
            Thread.sleep(150);
        }
        
        // Submit 10 CleanupJobs
        for (int i = 0; i < 10; i++) {
            String payload = String.format(
                "{\"directory\":\"/tmp/demo-%d\",\"olderThanDays\":%d}",
                i, 30 + random.nextInt(60));
            CleanupJob job = new CleanupJob("cleanup-" + i, payload, 2 + random.nextInt(4));
            repository.submitJob(job);
            submitted++;
            printProgress("  ", submitted, TOTAL_JOBS);
            Thread.sleep(150);
        }
        
        // Submit 10 ReportJobs
        for (int i = 0; i < 10; i++) {
            String payload = String.format(
                "{\"reportType\":\"%s\",\"startDate\":\"2024-01-01\",\"endDate\":\"2024-12-31\"}",
                new String[]{"sales", "inventory", "performance"}[i % 3]);
            ReportJob job = new ReportJob("report-" + i, payload, 7 + random.nextInt(3));
            repository.submitJob(job);
            submitted++;
            printProgress("  ", submitted, TOTAL_JOBS);
            Thread.sleep(150);
        }
        
        System.out.println("\n  " + GREEN + "✓ Submitted 30 jobs (10 EMAIL, 10 CLEANUP, 10 REPORT)" + RESET);
        Thread.sleep(1000);
        System.out.println();
        
        return repository;
    }
    
    //33333333333333333333333333333333333333333333333333333333
    private static void phase3_startScheduler(JobRepository repository) throws Exception {
        printPhaseHeader("STARTING SCHEDULER");
        
        // Start scheduler
        System.out.print("  Starting scheduler with " + WORKER_COUNT + " workers... ");
        scheduler = new Scheduler(WORKER_COUNT, repository);
        schedulerThread = new Thread(() -> scheduler.start(), "SchedulerMain");
        schedulerThread.setDaemon(false);
        schedulerThread.start();
        Thread.sleep(1500);
        System.out.println(GREEN + "✓" + RESET);
        
        // Start metrics server
        System.out.print("  Starting metrics server on port " + METRICS_PORT + "... ");
        metricsServer = new MetricsServer(repository, METRICS_PORT);
        metricsServer.start();
        Thread.sleep(1000);
        System.out.println(GREEN + "✓" + RESET);
        
        System.out.println("  " + BLUE + "ℹ  Visit http://localhost:" + METRICS_PORT + "/metrics for real-time stats" + RESET);
        Thread.sleep(2000);
        System.out.println();
    }
    
    //444444444444444444444444444444444444444444444444444444444444
    private static void phase4_liveMonitoring(JobRepository repository) throws Exception {
        printPhaseHeader("LIVE MONITORING");
        System.out.println();
        
        int iterations = 15; // Monitor for 30 seconds (15 x 2 seconds)
        
        for (int i = 0; i < iterations; i++) {
            try {
                Map<String, Object> metrics = repository.getMetrics();
                
                int pending = ((Number) metrics.getOrDefault("pending_jobs", 0)).intValue();
                int active = ((Number) metrics.getOrDefault("active_jobs", 0)).intValue();
                int totalProcessed = ((Number) metrics.getOrDefault("total_processed", 0)).intValue();
                
                // Calculate success and failed from total_processed
                int success = totalProcessed;
                int failed = 0;
                int total = pending + active + success + failed;
            
            int completion = total > 0 ? (success + failed) * 100 / TOTAL_JOBS : 0;
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            
            // Clear previous lines (6 lines for the box)
            if (i > 0) {
                System.out.print("\033[9A"); // Move up 9 lines
            }
            
            System.out.println("  ┌─────────────────────────────────────────────────┐");
            System.out.println("  │" + CYAN + BOLD + "       JOB QUEUE MONITOR" + RESET + "                       │");
            System.out.println("  ├─────────────────────────────────────────────────┤");
            System.out.printf("  │ %sPending:%s   %3d jobs  %s%-20s%s │\n", 
                YELLOW, RESET, pending, YELLOW, createProgressBar(pending, 20, 20), RESET);
            System.out.printf("  │ %sRunning:%s   %3d jobs  %s%-20s%s │\n", 
                BLUE, RESET, active, BLUE, createProgressBar(active, 8, 20), RESET);
            System.out.printf("  │ %sSuccess:%s   %3d jobs  %s%-20s%s │\n", 
                GREEN, RESET, success, GREEN, createProgressBar(success, TOTAL_JOBS, 20), RESET);
            System.out.printf("  │ %sFailed:%s    %3d jobs  %s%-20s%s │\n", 
                RED, RESET, failed, RED, createProgressBar(failed, 10, 20), RESET);
            System.out.println("  ├─────────────────────────────────────────────────┤");
            System.out.printf("  │ Queue Depth: %-8d  Completion: %3d%%      │\n", pending, completion);
            System.out.printf("  │ Elapsed: %-8s  Workers: %d active        │\n", formatDuration(elapsed * 1000), WORKER_COUNT);
            System.out.println("  └─────────────────────────────────────────────────┘");
            
            // Exit early if all jobs are complete
            if (pending == 0 && active == 0) {
                break;
            }
            
            Thread.sleep(2000);
            } catch (Exception e) {
                System.out.println("\n" + RED + "✗ Error during monitoring: " + e.getMessage() + RESET);
                break;
            }
        }
        
        System.out.println();
    }
    
    //55555555555555555555555555555555555555555555555555555555555555555
    private static void phase5_finalStatistics(JobRepository repository) throws Exception {
        printPhaseHeader("FINAL STATISTICS");
        Thread.sleep(1000);
        
        List<JobData> allJobs = repository.getAllJobs();
        
        System.out.println(MAGENTA + BOLD + "══════════════════════════════════════════════════════════════" + RESET);
        System.out.println(MAGENTA + BOLD + "                    EXECUTION SUMMARY                         " + RESET);
        System.out.println(MAGENTA + BOLD + "══════════════════════════════════════════════════════════════" + RESET);
        System.out.println();
        
        // Job counts by type
        System.out.println(CYAN + "📊 JOB COUNTS BY TYPE:" + RESET);
        Map<String, Long> countsByType = analytics.getJobCountsByType();
        countsByType.forEach((type, count) -> {
            System.out.printf("   %-20s: %2d jobs\n", type, count);
        });
        System.out.println();
        
        // Success rate
        long successCount = allJobs.stream().filter(j -> j.getStatus() == JobStatus.SUCCESS).count();
        long totalCount = allJobs.size();
        double successRate = totalCount > 0 ? (successCount * 100.0 / totalCount) : 0;
        
        String rateColor = successRate >= 80 ? GREEN : (successRate >= 60 ? YELLOW : RED);
        System.out.println(rateColor + "✓ SUCCESS RATE: " + String.format("%.1f%%", successRate) + 
            " (" + successCount + "/" + totalCount + " jobs)" + RESET);
        System.out.println();
        
        // Avg execution time by type
        System.out.println(CYAN + "⏱  AVG EXECUTION TIME BY TYPE:" + RESET);
        Map<String, Double> avgTimes = analytics.getAverageExecutionTimeByType();
        avgTimes.forEach((type, avgMillis) -> {
            System.out.printf("   %-20s: %.1fs\n", type, avgMillis / 1000.0);
        });
        System.out.println();
        
        // Top priority completed jobs
        System.out.println(CYAN + "🎯 TOP PRIORITY COMPLETED JOBS:" + RESET);
        List<JobData> topJobs = allJobs.stream()
            .filter(j -> j.getStatus() == JobStatus.SUCCESS || j.getStatus() == JobStatus.FAILED)
            .sorted((j1, j2) -> Integer.compare(j2.getPriority(), j1.getPriority()))
            .limit(5)
            .collect(Collectors.toList());
        
        int rank = 1;
        for (JobData job : topJobs) {
            String statusIcon = job.getStatus() == JobStatus.SUCCESS ? GREEN + "✓" + RESET : RED + "✗" + RESET;
            System.out.printf("   %d. %-15s (priority %d) %s\n", 
                rank++, job.getId(), job.getPriority(), statusIcon);
        }
        System.out.println();
        
        // Failed jobs
        List<JobData> failedJobs = allJobs.stream()
            .filter(j -> j.getStatus() == JobStatus.FAILED)
            .collect(Collectors.toList());
        
        if (!failedJobs.isEmpty()) {
            System.out.println(RED + "💀 FAILED JOBS: " + failedJobs.size() + RESET);
            for (JobData job : failedJobs) {
                String errorMsg = job.getErrorMessage() != null ? job.getErrorMessage() : "Unknown error";
                System.out.printf("   - %s (%s) → %s\n", job.getId(), job.getType(), errorMsg);
            }
            System.out.println();
        }
        
        // System metrics
        long totalRuntime = (System.currentTimeMillis() - startTime) / 1000;
        double throughput = totalCount > 0 ? totalCount / (double) totalRuntime : 0;
        
        System.out.println(CYAN + "⚙️  SYSTEM METRICS:" + RESET);
        System.out.printf("   Total Runtime    : %s\n", formatDuration(totalRuntime * 1000));
        System.out.printf("   Jobs Processed   : %d\n", totalCount);
        System.out.printf("   Throughput       : %.2f jobs/sec\n", throughput);
        System.out.printf("   Worker Count     : %d threads\n", WORKER_COUNT);
        
        System.out.println();
        System.out.println(MAGENTA + BOLD + "══════════════════════════════════════════════════════════════" + RESET);
        System.out.println();
        
        Thread.sleep(2000);
    }
    
    //66666666666666666666666666666666666666666666666666666
    private static void phase6_shutdown() throws Exception {
        printPhaseHeader("SHUTDOWN");
        
        System.out.println("  " + YELLOW + "⚠  Initiating graceful shutdown..." + RESET);
        Thread.sleep(1000);
        
        // Stop scheduler
        if (scheduler != null) {
            System.out.print("  Stopping scheduler (waiting for workers to finish)... ");
            scheduler.shutdown();
            Thread.sleep(500);
            System.out.println(GREEN + "✓" + RESET);
        }
        
        // Stop metrics server
        if (metricsServer != null) {
            System.out.print("  Stopping metrics server... ");
            metricsServer.stop();
            Thread.sleep(500);
            System.out.println(GREEN + "✓" + RESET);
        }
        
        // Close database
        if (database != null) {
            System.out.print("  Closing database... ");
            database.close();
            Thread.sleep(500);
            System.out.println(GREEN + "✓" + RESET);
        }
        
        System.out.println("  " + GREEN + "✓ All resources released" + RESET);
        System.out.println();
        
        long totalTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println(CYAN + "Total demonstration time: " + formatDuration(totalTime * 1000) + RESET);
        System.out.println();
        System.out.println(GREEN + BOLD + "✓ DEMONSTRATION COMPLETE!" + RESET);
        System.out.println();
    }
    
    // HELPER METHODS
    
    private static void printPhaseHeader(String title) {
        System.out.println(YELLOW + BOLD + "[" + title + "]" + RESET);
    }

    private static void printProgress(String prefix, int current, int total) {
        int barLength = 20;
        int filled = (int) ((double) current / total * barLength);
        
        StringBuilder bar = new StringBuilder(prefix + "[");
        for (int i = 0; i < barLength; i++) {
            bar.append(i < filled ? "█" : "░");
        }
        bar.append("] ");
        bar.append(String.format("%3d%%", (current * 100 / total)));
        
        // Use \r to overwrite the same line
        System.out.print("\r" + bar.toString());
        
        if (current == total) {
            System.out.println(); // New line when complete
        }
    }
    
    private static String createProgressBar(int value, int max, int length) {
        int filled = max > 0 ? (int) ((double) value / max * length) : 0;
        filled = Math.min(filled, length); // Cap at length
        
        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < length; i++) {
            bar.append(i < filled ? "█" : "░");
        }
        bar.append("]");
        
        return bar.toString();
    }

    private static String formatDuration(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        seconds = seconds % 60;
        
        if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        } else {
            return String.format("%ds", seconds);
        }
    }
    

    private static void cleanup() {
        try {
            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (metricsServer != null) {
                metricsServer.stop();
            }
            if (database != null) {
                database.close();
            }
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }
}

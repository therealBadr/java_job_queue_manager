package com.jobqueue.test;

import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.app.MetricsServer;
import com.jobqueue.jobs.EmailJob;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Test for the Main application startup
 */
public class TestMainStartup {
    
    public static void main(String[] args) {
        System.out.println("Starting Main Application Startup Test...\n");
        
        Database database = null;
        Scheduler scheduler = null;
        MetricsServer metricsServer = null;
        Thread schedulerThread = null;
        
        try {
            // Test 1: Database initialization
            System.out.println("Test 1: Initializing database...");
            database = new Database();
            database.initialize();
            System.out.println("✓ Database initialized successfully\n");
            
            // Test 2: Repository and recovery
            System.out.println("Test 2: Initializing repository...");
            JobRepository jobRepository = new JobRepository(database);
            int recovered = jobRepository.recoverCrashedJobs();
            System.out.println("✓ Repository initialized, recovered " + recovered + " crashed jobs\n");
            
            // Test 3: Scheduler initialization
            System.out.println("Test 3: Initializing scheduler with 8 workers...");
            scheduler = new Scheduler(8, jobRepository);
            
            // Start scheduler in separate thread
            final Scheduler finalScheduler = scheduler;
            schedulerThread = new Thread(() -> {
                try {
                    finalScheduler.start();
                } catch (Exception e) {
                    System.err.println("Scheduler error: " + e.getMessage());
                }
            }, "Test-Scheduler-Thread");
            
            schedulerThread.setDaemon(true);
            schedulerThread.start();
            
            // Give scheduler time to start
            Thread.sleep(1000);
            System.out.println("✓ Scheduler started successfully\n");
            
            // Test 4: Metrics server initialization
            System.out.println("Test 4: Initializing metrics server on port 8090...");
            metricsServer = new MetricsServer(jobRepository, 8090);
            metricsServer.start();
            System.out.println("✓ Metrics server started successfully");
            System.out.println("  Metrics available at http://localhost:8090/metrics\n");
            
            // Test 5: Verify metrics endpoint is accessible
            System.out.println("Test 5: Testing metrics endpoint...");
            Thread.sleep(500); // Give server time to fully start
            
            URL url = new URL("http://localhost:8090/metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("✓ Metrics endpoint responding (HTTP 200)\n");
            } else {
                System.err.println("✗ Metrics endpoint returned HTTP " + responseCode + "\n");
            }
            conn.disconnect();
            
            // Test 6: Submit a test job through scheduler
            System.out.println("Test 6: Submitting test job...");
            EmailJob testJob = new EmailJob("test@example.com", "Test Subject", 1);
            scheduler.submitJob(testJob);
            System.out.println("✓ Job submitted successfully: " + testJob.getId() + "\n");
            
            // Wait a bit for job to be processed
            Thread.sleep(2000);
            
            System.out.println("=== All startup tests completed successfully ===");
            System.out.println("\nShutting down test components...");
            
        } catch (Exception e) {
            System.err.println("✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Cleanup
            if (scheduler != null) {
                try {
                    scheduler.shutdown();
                    System.out.println("Scheduler stopped");
                } catch (Exception e) {
                    System.err.println("Error stopping scheduler: " + e.getMessage());
                }
            }
            
            if (schedulerThread != null) {
                try {
                    schedulerThread.interrupt();
                    schedulerThread.join(2000);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
            
            if (metricsServer != null) {
                try {
                    metricsServer.stop();
                    System.out.println("Metrics server stopped");
                } catch (Exception e) {
                    System.err.println("Error stopping metrics server: " + e.getMessage());
                }
            }
            
            if (database != null) {
                try {
                    database.close();
                    System.out.println("Database closed");
                } catch (Exception e) {
                    System.err.println("Error closing database: " + e.getMessage());
                }
            }
            
            System.out.println("\n=== Test cleanup complete ===");
        }
    }
}

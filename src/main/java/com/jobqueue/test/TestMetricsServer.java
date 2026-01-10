package com.jobqueue.test;

import com.jobqueue.app.MetricsServer;
import com.jobqueue.core.JobStatus;
import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.jobs.EmailJob;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Test suite for MetricsServer functionality
 */
public class TestMetricsServer {
    
    public static void main(String[] args) throws Exception {
        System.out.println("Starting MetricsServer Tests...\n");
        
        int passed = 0;
        int total = 0;
        
        // Test 1: Server starts successfully
        total++;
        try {
            testServerStart();
            System.out.println("✓ Test 1: Server starts successfully - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 1: Server starts successfully - FAILED");
            e.printStackTrace();
        }
        
        // Test 2: Metrics endpoint returns 200
        total++;
        try {
            testMetricsEndpoint();
            System.out.println("✓ Test 2: Metrics endpoint returns 200 - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 2: Metrics endpoint returns 200 - FAILED");
            e.printStackTrace();
        }
        
        // Test 3: Metrics JSON contains expected fields
        total++;
        try {
            testMetricsJsonStructure();
            System.out.println("✓ Test 3: Metrics JSON contains expected fields - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 3: Metrics JSON contains expected fields - FAILED");
            e.printStackTrace();
        }
        
        // Test 4: Metrics values are accurate
        total++;
        try {
            testMetricsAccuracy();
            System.out.println("✓ Test 4: Metrics values are accurate - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 4: Metrics values are accurate - FAILED");
            e.printStackTrace();
        }
        
        // Test 5: Server stops gracefully
        total++;
        try {
            testServerStop();
            System.out.println("✓ Test 5: Server stops gracefully - PASSED");
            passed++;
        } catch (AssertionError | Exception e) {
            System.err.println("✗ Test 5: Server stops gracefully - FAILED");
            e.printStackTrace();
        }
        
        System.out.println("\n" + passed + "/" + total + " MetricsServer tests passed");
        
        if (passed != total) {
            throw new RuntimeException("Some tests failed");
        }
    }
    
    private static void testServerStart() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        MetricsServer server = new MetricsServer(repo, 8081);
        server.start();
        
        // Give server time to start
        Thread.sleep(500);
        
        server.stop();
        db.close();
    }
    
    private static void testMetricsEndpoint() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        MetricsServer server = new MetricsServer(repo, 8082);
        server.start();
        Thread.sleep(500);
        
        try {
            URL url = new URL("http://localhost:8082/metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            int responseCode = conn.getResponseCode();
            
            if (responseCode != 200) {
                throw new AssertionError("Expected response code 200, got " + responseCode);
            }
            
            String contentType = conn.getHeaderField("Content-Type");
            if (!contentType.contains("application/json")) {
                throw new AssertionError("Expected Content-Type to contain 'application/json', got " + contentType);
            }
            
            conn.disconnect();
        } finally {
            server.stop();
            db.close();
        }
    }
    
    private static void testMetricsJsonStructure() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        MetricsServer server = new MetricsServer(repo, 8083);
        server.start();
        Thread.sleep(500);
        
        try {
            URL url = new URL("http://localhost:8083/metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            in.close();
            conn.disconnect();
            
            String json = response.toString();
            
            // Check for required fields
            String[] requiredFields = {
                "total_jobs_processed",
                "pending_jobs",
                "running_jobs",
                "success_rate",
                "failure_rate",
                "average_execution_time_ms",
                "queue_depth",
                "active_workers",
                "uptime_seconds"
            };
            
            for (String field : requiredFields) {
                if (!json.contains("\"" + field + "\"")) {
                    throw new AssertionError("JSON missing required field: " + field);
                }
            }
            
        } finally {
            server.stop();
            db.close();
        }
    }
    
    private static void testMetricsAccuracy() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        // Submit some jobs
        for (int i = 0; i < 5; i++) {
            EmailJob job = new EmailJob("test" + i + "@example.com", "Subject " + i, "Body " + i);
            repo.submitJob(job);
            String jobId = job.getId();
            
            if (i < 3) {
                // Mark 3 as completed
                repo.updateJobStatus(jobId, JobStatus.SUCCESS, null);
            } else {
                // Leave 2 as pending
            }
        }
        
        MetricsServer server = new MetricsServer(repo, 8084);
        server.start();
        Thread.sleep(500);
        
        try {
            URL url = new URL("http://localhost:8084/metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            in.close();
            conn.disconnect();
            
            String json = response.toString();
            
            // Check that pending_jobs is 2
            if (!json.contains("\"pending_jobs\": 2")) {
                throw new AssertionError("Expected pending_jobs to be 2 in: " + json);
            }
            
            // Check that uptime_seconds is present and greater than 0
            if (!json.contains("\"uptime_seconds\":")) {
                throw new AssertionError("uptime_seconds not found in response");
            }
            
        } finally {
            server.stop();
            db.close();
        }
    }
    
    private static void testServerStop() throws Exception {
        Database db = new Database();
        db.initialize();
        JobRepository repo = new JobRepository(db);
        
        MetricsServer server = new MetricsServer(repo, 8085);
        server.start();
        Thread.sleep(500);
        
        server.stop();
        
        // Give server time to stop
        Thread.sleep(1000);
        
        // Try to connect - should fail
        boolean connectionFailed = false;
        try {
            URL url = new URL("http://localhost:8085/metrics");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(1000);
            conn.setRequestMethod("GET");
            conn.getResponseCode();
        } catch (Exception e) {
            connectionFailed = true;
        }
        
        if (!connectionFailed) {
            throw new AssertionError("Server should not be accessible after stop()");
        }
        
        db.close();
    }
}

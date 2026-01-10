package com.jobqueue.app;

import com.jobqueue.db.JobRepository;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP server that exposes job queue metrics via REST API.
 * Provides a /metrics endpoint that returns JSON-formatted metrics.
 */
public class MetricsServer {
    private static final Logger logger = Logger.getLogger(MetricsServer.class.getName());
    
    private final JobRepository repository;
    private final int port;
    private HttpServer server;
    private final long startTime;
    
    /**
     * Create a new metrics server.
     * 
     * @param repository the job repository to fetch metrics from
     * @param port the port number to listen on
     */
    public MetricsServer(JobRepository repository, int port) {
        this.repository = repository;
        this.port = port;
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * Start the HTTP server.
     * Creates the server, registers endpoints, and begins listening for requests.
     * 
     * @throws IOException if the server cannot be started
     */
    public void start() throws IOException {
        // Create HTTP server
        server = HttpServer.create(new InetSocketAddress(port), 0);
        
        // Register endpoints
        registerMetricsEndpoint();
        
        // Set executor for handling requests
        server.setExecutor(Executors.newFixedThreadPool(4));
        
        // Start the server
        server.start();
        
        logger.info("Metrics server started on port " + port);
        logger.info("Metrics endpoint: http://localhost:" + port + "/metrics");
    }
    
    /**
     * Stop the HTTP server.
     * Gracefully shuts down the server with a 2-second delay.
     */
    public void stop() {
        if (server != null) {
            server.stop(2); // 2 second delay
            logger.info("Metrics server stopped");
        }
    }
    
    /**
     * Register the /metrics endpoint handler.
     * Sets up the HTTP handler that returns JSON metrics.
     */
    private void registerMetricsEndpoint() {
        server.createContext("/metrics", new MetricsHandler());
        logger.info("Registered /metrics endpoint");
    }
    
    /**
     * HTTP handler for the /metrics endpoint.
     * Fetches metrics from the repository and returns them as JSON.
     */
    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Only allow GET requests
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method Not Allowed");
                return;
            }
            
            try {
                // Get metrics from repository
                Map<String, Object> metrics = repository.getMetrics();
                
                // Calculate uptime
                long uptimeSeconds = (System.currentTimeMillis() - startTime) / 1000;
                
                // Build JSON response
                String json = buildJsonResponse(metrics, uptimeSeconds);
                
                // Set response headers
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                
                // Send response
                byte[] response = json.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
                
                logger.fine("Served metrics request");
                
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Failed to fetch metrics", e);
                sendError(exchange, 500, "Internal Server Error: " + e.getMessage());
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Unexpected error handling request", e);
                sendError(exchange, 500, "Internal Server Error");
            }
        }
        
        /**
         * Build JSON response from metrics data.
         * 
         * @param metrics the metrics map from repository
         * @param uptimeSeconds server uptime in seconds
         * @return JSON string
         */
        private String buildJsonResponse(Map<String, Object> metrics, long uptimeSeconds) {
            StringBuilder json = new StringBuilder();
            json.append("{\n");
            
            // Extract metrics with defaults
            long totalProcessed = ((Number) metrics.getOrDefault("total_processed", 0)).longValue();
            long pendingJobs = ((Number) metrics.getOrDefault("pending_jobs", 0)).longValue();
            long activeJobs = ((Number) metrics.getOrDefault("active_jobs", 0)).longValue();
            double successRate = ((Number) metrics.getOrDefault("success_rate", 0.0)).doubleValue();
            double avgExecutionTime = ((Number) metrics.getOrDefault("average_execution_time", 0.0)).doubleValue();
            
            // Calculate failure rate
            double failureRate = 100.0 - successRate;
            
            // Build JSON (manual construction for simplicity, no external dependencies)
            json.append("  \"total_jobs_processed\": ").append(totalProcessed).append(",\n");
            json.append("  \"pending_jobs\": ").append(pendingJobs).append(",\n");
            json.append("  \"running_jobs\": ").append(activeJobs).append(",\n");
            json.append("  \"success_rate\": ").append(String.format("%.1f", successRate)).append(",\n");
            json.append("  \"failure_rate\": ").append(String.format("%.1f", failureRate)).append(",\n");
            json.append("  \"average_execution_time_ms\": ").append(String.format("%.0f", avgExecutionTime)).append(",\n");
            json.append("  \"queue_depth\": ").append(pendingJobs).append(",\n");
            json.append("  \"active_workers\": ").append(activeJobs).append(",\n");
            json.append("  \"uptime_seconds\": ").append(uptimeSeconds).append("\n");
            
            json.append("}");
            
            return json.toString();
        }
        
        /**
         * Send an error response.
         * 
         * @param exchange the HTTP exchange
         * @param statusCode HTTP status code
         * @param message error message
         * @throws IOException if response cannot be sent
         */
        private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
            String json = "{\"error\": \"" + message + "\"}";
            byte[] response = json.getBytes(StandardCharsets.UTF_8);
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.length);
            
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }
}

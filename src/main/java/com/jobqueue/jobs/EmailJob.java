package com.jobqueue.jobs;

import java.io.IOException;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

import com.jobqueue.core.Job;
import com.jobqueue.core.JobContext;

// Job for sending emails via email service
public class EmailJob implements Job {
    private final String id;
    private final String payload;
    private final int priority;
    private static final String TYPE = "EmailJob";

    // Create a new EmailJob
    public EmailJob(String id, String payload, int priority) {
        this.id = id;
        this.payload = payload;
        this.priority = priority;
    }

    // Default constructor with generated UUID and default priority
    public EmailJob() {
        this.id = UUID.randomUUID().toString();
        this.payload = null;
        this.priority = 5;
    }

    // Constructor with email data
    public EmailJob(String to, String subject, String body) {
        this.id = UUID.randomUUID().toString();
        this.priority = 5;
        
        // Build JSON payload
        JSONObject json = new JSONObject();
        json.put("to", to);
        json.put("subject", subject);
        json.put("body", body);
        this.payload = json.toString();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public int getMaxRetries() {
        return 3;
    }

    @Override
    public boolean isCancellable() {
        return true;
    }

    @Override
    public void execute(JobContext context) throws Exception {
        try {
            // Parse JSON payload
            if (payload == null || payload.isEmpty()) {
                throw new IllegalArgumentException("Email payload is null or empty");
            }

            JSONObject json = new JSONObject(payload);
            
            // Extract fields from JSON
            String to = json.getString("to");
            String subject = json.getString("subject");
            String body = json.getString("body");

            // Validate required fields
            if (to == null || to.isEmpty()) {
                throw new IllegalArgumentException("Email recipient 'to' field is required");
            }

            // Log connection to email server
            context.log("INFO", "Connecting to email server");
            
            // Simulate network delay
            Thread.sleep(2000);
            
            // Check for cancellation
            context.throwIfCancelled();
            
            // Log sending email
            context.log("INFO", "Sending email to " + to);
            
            // Simulate email send operation
            Thread.sleep(1000);
            
            // Check for cancellation again
            context.throwIfCancelled();
            
            // Log success
            context.log("INFO", "Email sent successfully");
            
            // Add metadata
            context.addMetadata("recipient", to);
            context.addMetadata("subject", subject);
            context.addMetadata("emailSent", true);
            
        } catch (JSONException e) {
            context.log("ERROR", "Failed to parse email payload: " + e.getMessage());
            throw new IOException("Invalid JSON payload for email job", e);
        } catch (InterruptedException e) {
            context.log("WARN", "Email job was cancelled");
            throw e;
        } catch (Exception e) {
            context.log("ERROR", "Unexpected error during email send: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public String toString() {
        return "EmailJob{id='" + id + "', priority=" + priority + "}";
    }
}

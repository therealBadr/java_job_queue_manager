package com.jobqueue.jobs;

import com.jobqueue.core.BaseJob;
import com.jobqueue.core.JobContext;

import java.util.logging.Logger;

/**
 * Job for generating reports
 */
public class ReportJob extends BaseJob {
    private static final Logger logger = Logger.getLogger(ReportJob.class.getName());

    public static class ReportData {
        public String reportType;
        public String startDate;
        public String endDate;

        public ReportData() {}

        public ReportData(String reportType, String startDate, String endDate) {
            this.reportType = reportType;
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }

    public ReportJob() {
        super();
        setMaxRetries(2);
    }

    /**
     * Constructor for scheduler instantiation
     * @param id job ID
     * @param payload JSON payload
     * @param priority job priority
     */
    public ReportJob(String id, String payload, int priority) {
        super(id, payload, priority);
        setMaxRetries(2);
    }

    public void setReportData(String reportType, String startDate, String endDate) {
        ReportData data = new ReportData(reportType, startDate, endDate);
        setPayload(toPayload(data));
    }

    @Override
    public void execute(JobContext context) throws Exception {
        logger.info("Executing ReportJob: " + getId());
        
        // Parse report data from payload
        ReportData data = fromPayload(getPayload(), ReportData.class);
        
        if (data == null || data.reportType == null) {
            throw new IllegalArgumentException("Invalid report data");
        }

        // Log start of report generation
        context.log("INFO", "Generating " + data.reportType + " report");
        logger.info("Generating report type: " + data.reportType);
        logger.info("Date range: " + data.startDate + " to " + data.endDate);
        
        // Simulate data gathering
        context.updateProgress("Gathering data...");
        Thread.sleep(3000);
        
        // Check cancellation after data gathering
        context.throwIfCancelled();
        
        // Phase 1: Processing records
        context.updateProgress("Processing records...");
        context.log("INFO", "Processing records for " + data.reportType + " report");
        Thread.sleep(2000);
        
        // Check cancellation
        context.throwIfCancelled();
        
        // Phase 2: Calculating totals
        context.updateProgress("Calculating totals...");
        context.log("INFO", "Calculating totals for " + data.reportType + " report");
        Thread.sleep(2000);
        
        // Check cancellation
        context.throwIfCancelled();
        
        // Phase 3: Formatting output
        context.updateProgress("Formatting output...");
        context.log("INFO", "Formatting output for " + data.reportType + " report");
        Thread.sleep(1000);
        
        // Final cancellation check
        context.throwIfCancelled();
        
        // Log completion
        context.log("INFO", "Report generation completed");
        logger.info("Report generation completed for: " + data.reportType);
        
        // Add metadata to context
        context.addMetadata("reportType", data.reportType);
        context.addMetadata("startDate", data.startDate);
        context.addMetadata("endDate", data.endDate);
        context.addMetadata("completed", true);
    }

    @Override
    public int getPriority() {
        return 7; // High priority
    }
}

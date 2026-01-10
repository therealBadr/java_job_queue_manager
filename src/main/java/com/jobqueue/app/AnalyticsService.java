package com.jobqueue.app;

import com.jobqueue.core.JobStatus;
import com.jobqueue.db.JobRepository;
import com.jobqueue.db.JobRepository.JobData;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Analytics service for job metrics and insights using Java Streams API.
 * Provides various aggregations and statistics on job data.
 */
public class AnalyticsService {
    private final JobRepository repository;

    public AnalyticsService(JobRepository repository) {
        this.repository = repository;
    }

    /**
     * Get all jobs filtered by status.
     * 
     * @param status the job status to filter by
     * @return list of jobs with the specified status
     * @throws SQLException if database operation fails
     */
    public List<JobData> getJobsByStatus(JobStatus status) throws SQLException {
        return repository.getAllJobs().stream()
                .filter(job -> job.getStatus() == status)
                .collect(Collectors.toList());
    }

    /**
     * Count jobs grouped by type.
     * 
     * @return map of job type to count
     * @throws SQLException if database operation fails
     */
    public Map<String, Long> getJobCountsByType() throws SQLException {
        return repository.getAllJobs().stream()
                .collect(Collectors.groupingBy(
                        JobData::getType,
                        Collectors.counting()
                ));
    }

    /**
     * Calculate average execution time by job type.
     * Only includes completed jobs (SUCCESS or FAILED) with valid timestamps.
     * 
     * @return map of job type to average execution time in milliseconds
     * @throws SQLException if database operation fails
     */
    public Map<String, Double> getAverageExecutionTimeByType() throws SQLException {
        return repository.getAllJobs().stream()
                .filter(job -> (job.getStatus() == JobStatus.SUCCESS || job.getStatus() == JobStatus.FAILED))
                .filter(job -> job.getStartedAt() != null && job.getCompletedAt() != null)
                .collect(Collectors.groupingBy(
                        JobData::getType,
                        Collectors.averagingLong(job -> 
                            Duration.between(job.getStartedAt(), job.getCompletedAt()).toMillis()
                        )
                ));
    }

    /**
     * Get top priority pending jobs.
     * 
     * @param limit maximum number of jobs to return
     * @return list of pending jobs ordered by priority (highest first)
     * @throws SQLException if database operation fails
     */
    public List<JobData> getTopPriorityPendingJobs(int limit) throws SQLException {
        return repository.getAllJobs().stream()
                .filter(job -> job.getStatus() == JobStatus.PENDING)
                .sorted((j1, j2) -> Integer.compare(j2.getPriority(), j1.getPriority()))
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Calculate failure rate by job type as a percentage.
     * 
     * @return map of job type to failure percentage (0-100)
     * @throws SQLException if database operation fails
     */
    public Map<String, Double> getFailureRateByType() throws SQLException {
        List<JobData> allJobs = repository.getAllJobs();
        
        // Group by type and count total and failed
        Map<String, Long> totalByType = allJobs.stream()
                .collect(Collectors.groupingBy(
                        JobData::getType,
                        Collectors.counting()
                ));
        
        Map<String, Long> failedByType = allJobs.stream()
                .filter(job -> job.getStatus() == JobStatus.FAILED)
                .collect(Collectors.groupingBy(
                        JobData::getType,
                        Collectors.counting()
                ));
        
        // Calculate failure rate percentage for each type
        return totalByType.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            String type = entry.getKey();
                            long total = entry.getValue();
                            long failed = failedByType.getOrDefault(type, 0L);
                            return (failed * 100.0) / total;
                        }
                ));
    }

    /**
     * Count jobs completed in the last hour.
     * 
     * @return number of jobs completed in the last hour
     * @throws SQLException if database operation fails
     */
    public long getJobsCompletedInLastHour() throws SQLException {
        LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);
        
        return repository.getAllJobs().stream()
                .filter(job -> job.getCompletedAt() != null)
                .filter(job -> job.getCompletedAt().isAfter(oneHourAgo))
                .count();
    }
}

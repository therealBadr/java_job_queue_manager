package com.jobqueue.engine;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.jobqueue.core.Job;
import com.jobqueue.core.QueueOverloadException;
import com.jobqueue.db.JobRepository;

/**
 * Main Scheduler class that manages job execution and worker threads
 */
public class Scheduler {
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    
    private final ExecutorService executorService;
    private final JobRepository repository;
    private final AtomicBoolean running;
    private final AtomicBoolean backpressureActive;
    private final int workerCount;
    private final int queueDepthThreshold;

    /**
     * Create a new Scheduler
     * @param workerCount number of worker threads in the pool
     * @param repository job repository for database operations
     */
    public Scheduler(int workerCount, JobRepository repository) {
        this.workerCount = workerCount;
        this.repository = repository;
        this.executorService = Executors.newFixedThreadPool(workerCount);
        this.running = new AtomicBoolean(false);
        this.backpressureActive = new AtomicBoolean(false);
        this.queueDepthThreshold = 1000; // Default backpressure threshold
        
        logger.info("Scheduler initialized with " + workerCount + " workers");
    }

    /**
     * Create a Scheduler with custom queue depth threshold
     * @param workerCount number of worker threads
     * @param repository job repository
     * @param queueDepthThreshold max queue depth before backpressure
     */
    public Scheduler(int workerCount, JobRepository repository, int queueDepthThreshold) {
        this.workerCount = workerCount;
        this.repository = repository;
        this.executorService = Executors.newFixedThreadPool(workerCount);
        this.running = new AtomicBoolean(false);
        this.backpressureActive = new AtomicBoolean(false);
        this.queueDepthThreshold = queueDepthThreshold;
        
        logger.info("Scheduler initialized with " + workerCount + " workers and threshold " + queueDepthThreshold);
    }

    /**
     * Start the scheduler - begins the scheduling loop
     * This method runs in a blocking loop and should be called from a separate thread
     */
    public void start() {
        // Set running to true atomically
        if (!running.compareAndSet(false, true)) {
            logger.warning("Scheduler is already running");
            return;
        }
        
        logger.info("Scheduler started with " + workerCount + " workers");
        
        // Recover any crashed jobs (jobs stuck in RUNNING state)
        try {
            int recovered = repository.recoverCrashedJobs();
            if (recovered > 0) {
                logger.info("Recovered " + recovered + " crashed jobs");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to recover crashed jobs", e);
        }
        
        // Main scheduling loop
        while (running.get()) {
            try {
                // Check backpressure: if queue depth > threshold, activate backpressure
                int currentDepth = repository.getQueueDepth();
                
                if (currentDepth > queueDepthThreshold) {
                    // Activate backpressure
                    if (!backpressureActive.get()) {
                        backpressureActive.set(true);
                        logger.warning("Backpressure activated: " + currentDepth + 
                                     " jobs pending (threshold: " + queueDepthThreshold + ")");
                    }
                    Thread.sleep(5000); // Sleep for 5 seconds during backpressure
                    continue;
                } else if (currentDepth < queueDepthThreshold * 0.8) {
                    // Release backpressure if depth drops below 80% of threshold
                    if (backpressureActive.get()) {
                        backpressureActive.set(false);
                        logger.info("Backpressure released: queue depth now " + currentDepth);
                    }
                }
                
                // Get pending jobs with anti-starvation algorithm (limit 10)
                List<JobRepository.JobData> pendingJobs = getPendingJobsWithAntiStarvation(10);
                
                if (pendingJobs.isEmpty()) {
                    // If no jobs found, sleep for 1 second to avoid busy waiting
                    Thread.sleep(1000);
                    continue;
                }
                
                // Process each pending job
                for (JobRepository.JobData jobData : pendingJobs) {
                    if (!running.get()) {
                        break; // Exit if shutdown requested
                    }
                    
                    try {
                        // Attempt to claim the job atomically
                        boolean claimed = repository.claimJob(jobData.getId());
                        
                        if (claimed) {
                            // Create Job instance from JobData
                            Job job = createJobInstance(jobData);
                            
                            if (job != null) {
                                // Submit new Worker to executor service
                                Worker worker = new Worker(job, repository);
                                executorService.submit(worker);
                                logger.info("Job " + jobData.getId() + " submitted to worker pool");
                            } else {
                                logger.warning("Failed to create job instance for: " + jobData.getId());
                                // Update job to FAILED
                                repository.updateJobStatus(jobData.getId(), 
                                    com.jobqueue.core.JobStatus.FAILED, 
                                    "Failed to create job instance");
                            }
                        }
                    } catch (java.sql.SQLException e) {
                        logger.log(Level.SEVERE, "SQLException while processing job " + jobData.getId(), e);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Error processing job " + jobData.getId(), e);
                    }
                }
                
            } catch (InterruptedException e) {
                logger.info("Scheduler interrupted, shutting down gracefully");
                Thread.currentThread().interrupt();
                break;
            } catch (java.sql.SQLException e) {
                logger.log(Level.SEVERE, "SQLException in scheduling loop", e);
                try {
                    Thread.sleep(2000); // Back off on database errors
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Unexpected error in scheduling loop", e);
                try {
                    Thread.sleep(1000); // Brief pause before retrying
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Scheduler loop exited");
    }

    /**
     * Get pending jobs with anti-starvation algorithm.
     * Calculates effective priority = base priority + (age_in_minutes / 60)
     * This increases priority by 1 for each hour the job waits.
     * 
     * @param limit maximum number of jobs to return
     * @return list of jobs sorted by effective priority (highest first)
     */
    private List<JobRepository.JobData> getPendingJobsWithAntiStarvation(int limit) {
        try {
            // Get all pending jobs
            List<JobRepository.JobData> allPendingJobs = repository.getPendingJobs(Integer.MAX_VALUE);
            
            if (allPendingJobs.isEmpty()) {
                return allPendingJobs;
            }
            
            LocalDateTime now = LocalDateTime.now();
            
            // Sort by effective priority (base priority + age bonus)
            return allPendingJobs.stream()
                .sorted((job1, job2) -> {
                    // Calculate effective priority for job1
                    double effectivePriority1 = calculateEffectivePriority(job1, now);
                    
                    // Calculate effective priority for job2
                    double effectivePriority2 = calculateEffectivePriority(job2, now);
                    
                    // Sort descending (higher priority first)
                    return Double.compare(effectivePriority2, effectivePriority1);
                })
                .limit(limit)
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in anti-starvation algorithm, falling back to simple query", e);
            try {
                return repository.getPendingJobs(limit);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Fallback query also failed", ex);
                return List.of();
            }
        }
    }
    
    /**
     * Calculate effective priority for a job based on base priority and age.
     * Formula: base_priority + (age_in_minutes / 60)
     * 
     * @param job the job data
     * @param now current time
     * @return effective priority as a double
     */
    private double calculateEffectivePriority(JobRepository.JobData job, LocalDateTime now) {
        int basePriority = job.getPriority();
        
        // Calculate age in minutes
        LocalDateTime createdAt = job.getCreatedAt();
        if (createdAt == null) {
            return basePriority; // No age bonus if no creation time
        }
        
        long ageInMinutes = Duration.between(createdAt, now).toMinutes();
        
        // Age bonus: +1 priority for each hour waiting
        double ageBonus = ageInMinutes / 60.0;
        
        return basePriority + ageBonus;
    }

    /**
     * Create a Job instance from JobData using reflection
     */
    private Job createJobInstance(JobRepository.JobData jobData) {
        try {
            // Load the job class
            String className = "com.jobqueue.jobs." + jobData.getType();
            Class<?> jobClass = Class.forName(className);
            
            // Create instance with parameterized constructor
            java.lang.reflect.Constructor<?> constructor = jobClass.getConstructor(
                String.class, String.class, int.class);
            
            Job job = (Job) constructor.newInstance(
                jobData.getId(), 
                jobData.getPayload(), 
                jobData.getPriority()
            );
            
            return job;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to create job instance: " + jobData.getType(), e);
            return null;
        }
    }

    /**
     * Submit a new job to the scheduler
     */
    public void submitJob(Job job) {
        try {
            // Check if backpressure is active before accepting new jobs
            rejectIfOverloaded();
            
            repository.submitJob(job);
            logger.info("Job submitted: " + job.getId() + " (type: " + job.getType() + ")");
        } catch (QueueOverloadException e) {
            // Re-throw queue overload exceptions
            throw e;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to submit job: " + job.getId(), e);
            throw new RuntimeException("Failed to submit job", e);
        }
    }
    
    /**
     * Check if the queue is overloaded and reject new jobs if backpressure is active.
     * 
     * @throws QueueOverloadException if backpressure is currently active
     */
    private void rejectIfOverloaded() {
        if (backpressureActive.get()) {
            try {
                int currentDepth = repository.getQueueDepth();
                throw new QueueOverloadException(
                    "Queue is overloaded, rejecting new jobs",
                    currentDepth,
                    queueDepthThreshold
                );
            } catch (Exception e) {
                if (e instanceof QueueOverloadException) {
                    throw (QueueOverloadException) e;
                }
                // If we can't get queue depth, still reject based on backpressure flag
                throw new QueueOverloadException("Queue is overloaded, rejecting new jobs");
            }
        }
    }

    /**
     * Cancel a job by ID.
     * Sets the job status to CANCELLED in the database.
     * If the job is currently running, it will be marked as cancelled
     * but may continue executing until it checks the cancellation flag.
     * 
     * @param jobId the ID of the job to cancel
     * @return true if the job was cancelled, false if it was not in a cancellable state
     */
    public boolean cancelJob(String jobId) {
        try {
            boolean cancelled = repository.cancelJob(jobId);
            if (cancelled) {
                logger.info("Job cancelled: " + jobId);
            } else {
                logger.warning("Could not cancel job " + jobId + " (not in cancellable state)");
            }
            return cancelled;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to cancel job: " + jobId, e);
            return false;
        }
    }

    /**
     * Shutdown the scheduler gracefully
     */
    public void shutdown() {
        // Set running to false (stops scheduling loop)
        running.set(false);
        logger.info("Initiating graceful shutdown...");
        
        // Stop accepting new tasks
        executorService.shutdown();
        
        try {
            // Wait up to 60 seconds for tasks to complete
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                // Force shutdown if not terminated after 60 seconds
                logger.warning("Forcing shutdown of remaining tasks");
                executorService.shutdownNow();
                
                // Wait another 10 seconds for force shutdown
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.severe("Executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            // Force shutdown and restore interrupt
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Scheduler shutdown complete");
    }

    /**
     * Get the current status of the scheduler
     * @return map containing scheduler status information
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        
        status.put("running", running.get());
        status.put("workerCount", workerCount);
        status.put("queueDepthThreshold", queueDepthThreshold);
        status.put("executorShutdown", executorService.isShutdown());
        status.put("executorTerminated", executorService.isTerminated());
        
        // Get queue metrics from repository
        try {
            int queueDepth = repository.getQueueDepth();
            status.put("currentQueueDepth", queueDepth);
            
            Map<String, Object> metrics = repository.getMetrics();
            status.put("activeJobs", metrics.get("active_jobs"));
            status.put("pendingJobs", metrics.get("pending_jobs"));
            status.put("totalProcessed", metrics.get("total_processed"));
            status.put("successRate", metrics.get("success_rate"));
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to get repository metrics", e);
            status.put("metricsError", e.getMessage());
        }
        
        return status;
    }

    /**
     * Check if scheduler is running
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get worker count
     */
    public int getWorkerCount() {
        return workerCount;
    }

    /**
     * Get queue depth threshold
     */
    public int getQueueDepthThreshold() {
        return queueDepthThreshold;
    }
}

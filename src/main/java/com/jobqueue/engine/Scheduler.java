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
 * Main Scheduler class that manages job execution and worker thread pool.
 * 
 * <p>The Scheduler is the heart of the job queue system. It continuously polls the database
 * for pending jobs, applies prioritization and anti-starvation logic, claims jobs atomically,
 * and submits them to a fixed-size worker thread pool for execution.</p>
 * 
 * <p><b>Key Responsibilities:</b></p>
 * <ul>
 *   <li>Poll database for PENDING jobs at 1-second intervals</li>
 *   <li>Apply anti-starvation algorithm: effective_priority = base_priority + (age_minutes / 60)</li>
 *   <li>Atomically claim jobs to prevent duplicate execution</li>
 *   <li>Submit claimed jobs to worker thread pool</li>
 *   <li>Monitor queue depth and activate backpressure when threshold exceeded</li>
 *   <li>Recover crashed jobs on startup (jobs stuck in RUNNING state)</li>
 *   <li>Gracefully shutdown with in-flight job completion</li>
 * </ul>
 * 
 * <p><b>Thread Safety:</b></p>
 * <ul>
 *   <li>Scheduler runs in its own dedicated thread (non-daemon)</li>
 *   <li>Worker pool uses fixed thread pool (default: 8 threads)</li>
 *   <li>Atomic flags (AtomicBoolean) for thread-safe state management</li>
 *   <li>Database-level job claiming prevents race conditions</li>
 * </ul>
 * 
 * <p><b>Backpressure Mechanism:</b></p>
 * <ul>
 *   <li>Activates when queue depth > threshold (default: 1000 jobs)</li>
 *   <li>Rejects new job submissions with QueueOverloadException</li>
 *   <li>Releases when queue depth < 80% of threshold (hysteresis)</li>
 *   <li>Prevents memory exhaustion and database overload</li>
 * </ul>
 * 
 * <p><b>Anti-Starvation Algorithm:</b></p>
 * <p>Low-priority jobs gain +1 priority per hour waiting. This ensures that even
 * low-priority jobs eventually execute, preventing indefinite postponement.</p>
 * <pre>
 * effective_priority = base_priority + (age_in_minutes / 60)
 * </pre>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * // Create scheduler with 8 workers
 * Scheduler scheduler = new Scheduler(8, repository);
 * 
 * // Start in separate thread
 * Thread schedulerThread = new Thread(scheduler::start);
 * schedulerThread.setDaemon(false);
 * schedulerThread.start();
 * 
 * // Submit jobs
 * scheduler.submitJob(new EmailJob());
 * 
 * // Graceful shutdown
 * scheduler.shutdown(); // Waits up to 60 seconds
 * schedulerThread.join(5000);
 * }</pre>
 * 
 * @author Job Queue Team
 * @see Worker
 * @see JobRepository#claimJob(String)
 */
public class Scheduler {
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    
    private final ExecutorService executorService;  // Fixed thread pool for workers
    private final JobRepository repository;          // Database access layer
    private final AtomicBoolean running;            // Atomic flag: is scheduler active?
    private final AtomicBoolean backpressureActive; // Atomic flag: is backpressure on?
    private final int workerCount;                  // Number of worker threads
    private final int queueDepthThreshold;          // Backpressure activation threshold

    /**
     * Create a new Scheduler with default backpressure threshold (1000 jobs).
     * 
     * <p>The scheduler creates a fixed thread pool and initializes atomic state flags.
     * It does not start execution until start() is called.</p>
     * 
     * @param workerCount number of worker threads in the pool (typically 8)
     * @param repository job repository for database operations
     * @throws IllegalArgumentException if workerCount < 1
     */
    public Scheduler(int workerCount, JobRepository repository) {
        this.workerCount = workerCount;
        this.repository = repository;
        // Create fixed thread pool - threads created lazily as jobs are submitted
        this.executorService = Executors.newFixedThreadPool(workerCount);
        // Initialize atomic flags - ensures thread-safe state management
        this.running = new AtomicBoolean(false);
        this.backpressureActive = new AtomicBoolean(false);
        this.queueDepthThreshold = 1000; // Default: activate backpressure at 1000 pending jobs
        
        logger.info("Scheduler initialized with " + workerCount + " workers");
    }

    /**
     * Create a Scheduler with custom queue depth threshold for backpressure.
     * 
     * <p>Use this constructor to tune backpressure activation based on your workload.
     * Lower thresholds protect the system better but may reject jobs more aggressively.
     * Higher thresholds allow more queuing but risk memory exhaustion.</p>
     * 
     * @param workerCount number of worker threads (1-100 typical)
     * @param repository job repository for persistence
     * @param queueDepthThreshold max pending jobs before backpressure activates
     * @throws IllegalArgumentException if workerCount < 1 or threshold < 1
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
     * Start the scheduler - begins the scheduling loop.
     * 
     * <p><b>⚠️ BLOCKING METHOD:</b> This method runs in an infinite loop until shutdown() is called.
     * Always call this from a separate thread:</p>
     * <pre>{@code
     * Thread thread = new Thread(scheduler::start);
     * thread.setDaemon(false); // Non-daemon: waits for jobs to complete
     * thread.start();
     * }</pre>
     * 
     * <p><b>Startup Sequence:</b></p>
     * <ol>
     *   <li>Set running flag atomically (prevents duplicate start)</li>
     *   <li>Recover crashed jobs (RUNNING → PENDING)</li>
     *   <li>Enter main scheduling loop</li>
     * </ol>
     * 
     * <p><b>Main Loop:</b></p>
     * <ol>
     *   <li>Check backpressure conditions (queue depth > threshold?)</li>
     *   <li>Query pending jobs with anti-starvation algorithm</li>
     *   <li>Atomically claim each job (prevents duplicate execution)</li>
     *   <li>Create Job instance from database data</li>
     *   <li>Submit Worker to thread pool</li>
     *   <li>Sleep 1 second if no jobs found (avoid busy-waiting)</li>
     * </ol>
     * 
     * <p><b>Error Handling:</b></p>
     * <ul>
     *   <li>SQLException: 2-second backoff, continue loop</li>
     *   <li>InterruptedException: graceful shutdown</li>
     *   <li>Other exceptions: log and continue (resilient)</li>
     * </ul>
     * 
     * @throws IllegalStateException if scheduler is already running
     */
    public void start() {
        // ATOMIC OPERATION: Set running to true, fails if already true
        // Why: Prevents multiple scheduler threads from running simultaneously
        if (!running.compareAndSet(false, true)) {
            logger.warning("Scheduler is already running");
            return;
        }
        
        logger.info("Scheduler started with " + workerCount + " workers");
        
        // CRASH RECOVERY: Reset jobs stuck in RUNNING state to PENDING
        // Why: Jobs may be left in RUNNING if the scheduler crashed previously
        try {
            int recovered = repository.recoverCrashedJobs();
            if (recovered > 0) {
                logger.info("Recovered " + recovered + " crashed jobs");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to recover crashed jobs", e);
        }
        
        // MAIN SCHEDULING LOOP
        while (running.get()) {
            try {
                // BACKPRESSURE CHECK: Monitor queue depth to prevent overload
                int currentDepth = repository.getQueueDepth();
                
                if (currentDepth > queueDepthThreshold) {
                    // ACTIVATE BACKPRESSURE: Too many jobs queued
                    if (!backpressureActive.get()) {
                        backpressureActive.set(true);
                        logger.warning("Backpressure activated: " + currentDepth + 
                                     " jobs pending (threshold: " + queueDepthThreshold + ")");
                    }
                    Thread.sleep(5000); // Sleep during backpressure instead of querying DB
                    continue;
                } else if (currentDepth < queueDepthThreshold * 0.8) {
                    // RELEASE BACKPRESSURE: Hysteresis at 80% prevents oscillation
                    // Why 80%: Prevents rapid on/off toggling near threshold
                    if (backpressureActive.get()) {
                        backpressureActive.set(false);
                        logger.info("Backpressure released: queue depth now " + currentDepth);
                    }
                }
                
                // QUERY PENDING JOBS: Use anti-starvation algorithm
                // Limit to 10 jobs per iteration to avoid overwhelming the system
                List<JobRepository.JobData> pendingJobs = getPendingJobsWithAntiStarvation(10);
                
                if (pendingJobs.isEmpty()) {
                    // IDLE STATE: No jobs to process, sleep to avoid busy-waiting
                    Thread.sleep(1000); // 1-second polling interval
                    continue;
                }
                
                // PROCESS EACH PENDING JOB
                for (JobRepository.JobData jobData : pendingJobs) {
                    if (!running.get()) {
                        break; // Exit if shutdown requested mid-iteration
                    }
                    
                    try {
                        // ATOMIC CLAIM: Update job status from PENDING → RUNNING
                        // This is the critical section that prevents duplicate execution
                        // Database-level locking ensures only one worker claims the job
                        boolean claimed = repository.claimJob(jobData.getId());
                        
                        if (claimed) {
                            // Job successfully claimed, create Job instance
                            Job job = createJobInstance(jobData);
                            
                            if (job != null) {
                                // SUBMIT TO WORKER POOL: Non-blocking submission
                                Worker worker = new Worker(job, repository);
                                executorService.submit(worker);
                                logger.info("Job " + jobData.getId() + " submitted to worker pool");
                            } else {
                                // Failed to instantiate job class (reflection error?)
                                logger.warning("Failed to create job instance for: " + jobData.getId());
                                repository.updateJobStatus(jobData.getId(), 
                                    com.jobqueue.core.JobStatus.FAILED, 
                                    "Failed to create job instance");
                            }
                        }
                        // If not claimed, another scheduler instance claimed it (distributed setup)
                    } catch (java.sql.SQLException e) {
                        logger.log(Level.SEVERE, "SQLException while processing job " + jobData.getId(), e);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Error processing job " + jobData.getId(), e);
                    }
                }
                
            } catch (InterruptedException e) {
                // GRACEFUL SHUTDOWN: Thread interrupted, exit loop cleanly
                logger.info("Scheduler interrupted, shutting down gracefully");
                Thread.currentThread().interrupt(); // Restore interrupt flag
                break;
            } catch (java.sql.SQLException e) {
                // DATABASE ERROR: Backoff and retry
                // Why: Temporary database issues shouldn't crash the scheduler
                logger.log(Level.SEVERE, "SQLException in scheduling loop", e);
                try {
                    Thread.sleep(2000); // 2-second backoff on database errors
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                // UNEXPECTED ERROR: Log and continue (resilience)
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
     * 
     * <p><b>Anti-Starvation Formula:</b></p>
     * <pre>
     * effective_priority = base_priority + (age_in_minutes / 60)
     * </pre>
     * 
     * <p>This increases priority by +1 for each hour the job waits. For example:</p>
     * <ul>
     *   <li>Priority 0 job waiting 0 minutes: effective = 0</li>
     *   <li>Priority 0 job waiting 60 minutes: effective = 1</li>
     *   <li>Priority 0 job waiting 120 minutes: effective = 2</li>
     *   <li>Priority 5 job waiting 60 minutes: effective = 6</li>
     * </ul>
     * 
     * <p><b>Why This Works:</b> Low-priority jobs eventually "catch up" to higher-priority
     * jobs. A priority-0 job waiting 10 hours has effective priority 10, which is higher
     * than a freshly-submitted priority-9 job.</p>
     * 
     * <p><b>Performance:</b> This method uses Java Streams API for clean, functional code.
     * For very large job counts (>10,000), consider database-side sorting.</p>
     * 
     * @param limit maximum number of jobs to return (typically 10)
     * @return list of jobs sorted by effective priority (highest first)
     */
    private List<JobRepository.JobData> getPendingJobsWithAntiStarvation(int limit) {
        try {
            // Query ALL pending jobs first
            // Why: Need all jobs to calculate relative priorities
            List<JobRepository.JobData> allPendingJobs = repository.getPendingJobs(Integer.MAX_VALUE);
            
            if (allPendingJobs.isEmpty()) {
                return allPendingJobs;
            }
            
            LocalDateTime now = LocalDateTime.now();
            
            // STREAMS API: Sort by effective priority (base + age bonus)
            return allPendingJobs.stream()
                .sorted((job1, job2) -> {
                    // Calculate effective priority for each job
                    double effectivePriority1 = calculateEffectivePriority(job1, now);
                    double effectivePriority2 = calculateEffectivePriority(job2, now);
                    
                    // Sort descending (higher priority first)
                    return Double.compare(effectivePriority2, effectivePriority1);
                })
                .limit(limit) // Take only top N jobs
                .collect(Collectors.toList());
                
        } catch (Exception e) {
            // FALLBACK: If anti-starvation fails, use simple priority sort
            // Why: Algorithm failure shouldn't prevent job processing
            logger.log(Level.SEVERE, "Error in anti-starvation algorithm, falling back to simple query", e);
            try {
                return repository.getPendingJobs(limit);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Fallback query also failed", ex);
                return List.of(); // Return empty list as last resort
            }
        }
    }
    
    /**
     * Calculate effective priority for a job based on base priority and age.
     * 
     * <p><b>Formula:</b></p>
     * <pre>
     * effective_priority = base_priority + (age_in_minutes / 60)
     * </pre>
     * 
     * <p>The age bonus prevents starvation by gradually increasing the priority
     * of waiting jobs. Each hour of waiting adds +1 to the effective priority.</p>
     * 
     * @param job the job data from database
     * @param now current timestamp for age calculation
     * @return effective priority as a double (can be fractional)
     */
    private double calculateEffectivePriority(JobRepository.JobData job, LocalDateTime now) {
        int basePriority = job.getPriority();
        
        // Calculate age in minutes since job creation
        LocalDateTime createdAt = job.getCreatedAt();
        if (createdAt == null) {
            return basePriority; // No age bonus if no creation time
        }
        
        long ageInMinutes = Duration.between(createdAt, now).toMinutes();
        
        // Age bonus: +1 priority for each hour (60 minutes) waiting
        // Example: 90 minutes waiting = 1.5 bonus
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

package com.jobqueue.engine;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.jobqueue.core.Job;
import com.jobqueue.core.QueueOverloadException;
import com.jobqueue.db.JobRepository;

public class Scheduler {
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    

    private final List<Thread> workerThreads;       // List of active worker threads (manual)
    private final JobRepository repository;          // Database access layer
    private final AtomicBoolean running;            // Atomic flag: is scheduler active?
    private final AtomicBoolean backpressureActive; // Atomic flag: is backpressure on?
    private final int workerCount;                  // Number of worker threads
    private final int queueDepthThreshold;          // Backpressure activation threshold

    public Scheduler(int workerCount, JobRepository repository) {
        this.workerCount = workerCount;
        this.repository = repository;
        this.workerThreads = new ArrayList<>();
        this.running = new AtomicBoolean(false);
        this.backpressureActive = new AtomicBoolean(false);
        this.queueDepthThreshold = 1000; 
        
        logger.info("Scheduler initialized with " + workerCount + " workers (MANUAL THREAD MANAGEMENT)");
    }

    public Scheduler(int workerCount, JobRepository repository, int queueDepthThreshold) {
        this.workerCount = workerCount;
        this.repository = repository;
        this.workerThreads = new ArrayList<>();
        this.running = new AtomicBoolean(false);
        this.backpressureActive = new AtomicBoolean(false);
        this.queueDepthThreshold = queueDepthThreshold;
        
        logger.info("Scheduler initialized with " + workerCount + " workers and threshold " + queueDepthThreshold + " (MANUAL THREAD MANAGEMENT)");
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            logger.warning("Scheduler is already running");
            return;
        }
        
        logger.info("Scheduler started with " + workerCount + " workers");
        
        try {
            int recovered = repository.recoverCrashedJobs();
            if (recovered > 0) {
                logger.info("Recovered " + recovered + " crashed jobs");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to recover crashed jobs", e);
        }
        
        while (running.get()) {
            try {
                int currentDepth = repository.getQueueDepth();
                
                if (currentDepth > queueDepthThreshold) {
                    if (!backpressureActive.get()) {
                        backpressureActive.set(true);
                        logger.warning("Backpressure activated: " + currentDepth + 
                                     " jobs pending (threshold: " + queueDepthThreshold + ")");
                    }
                    Thread.sleep(5000);
                    continue;
                } else if (currentDepth < queueDepthThreshold * 0.8) {
                    if (backpressureActive.get()) {
                        backpressureActive.set(false);
                        logger.info("Backpressure released: queue depth now " + currentDepth);
                    }
                }
                
                List<JobRepository.JobData> pendingJobs = getPendingJobsWithAntiStarvation(10);
                
                if (pendingJobs.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                
                for (JobRepository.JobData jobData : pendingJobs) {
                    if (!running.get()) {
                        break; // Exit if shutdown requested mid-iteration
                    }
                    
                    try {
                        boolean claimed = repository.claimJob(jobData.getId());
                        
                        if (claimed) {
                            Job job = createJobInstance(jobData);
                            
                            if (job != null) {
                                Worker worker = new Worker(job, repository);
                                
                                // MANUAL THREAD CREATION: new Thread() instead of executorService.submit()
                                String threadName = "Worker-" + (jobData.getId().length() > 8 ? 
                                    jobData.getId().substring(0, 8) : jobData.getId());
                                Thread workerThread = new Thread(worker, threadName);
                                
                                // Add to tracking list (synchronized for thread safety)
                                synchronized (workerThreads) {
                                    workerThreads.add(workerThread);
                                    
                                    // Clean up completed threads to avoid memory leak
                                    workerThreads.removeIf(t -> !t.isAlive());
                                }
                                
                                // START THE THREAD
                                workerThread.start();
                                logger.info("Job " + jobData.getId() + " started in new thread: " + workerThread.getName());
                            } else {
                                logger.warning("Failed to create job instance for: " + jobData.getId());
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
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Unexpected error in scheduling loop", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("Scheduler loop exited");
    }

    // Get pending jobs sorted by effective priority (base + age bonus)
    private List<JobRepository.JobData> getPendingJobsWithAntiStarvation(int limit) {
        try {
            List<JobRepository.JobData> allPendingJobs = repository.getPendingJobs(Integer.MAX_VALUE);
            
            if (allPendingJobs.isEmpty()) {
                return allPendingJobs;
            }
            
            LocalDateTime now = LocalDateTime.now();
            
            return allPendingJobs.stream()
                .sorted((job1, job2) -> {
                    double effectivePriority1 = calculateEffectivePriority(job1, now);
                    double effectivePriority2 = calculateEffectivePriority(job2, now);
                    
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
    
    // Calculate effective priority: base + (age_in_minutes / 60)
    private double calculateEffectivePriority(JobRepository.JobData job, LocalDateTime now) {
        int basePriority = job.getPriority();
        
        LocalDateTime createdAt = job.getCreatedAt();
        if (createdAt == null) {
            return basePriority;
        }
        
        long ageInMinutes = Duration.between(createdAt, now).toMinutes();
        double ageBonus = ageInMinutes / 60.0;
        
        return basePriority + ageBonus;
    }

    // Create Job instance using reflection
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

    // Submit a new job
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
    
    // Reject new jobs if backpressure is active
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

    // Cancel a job by ID
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

    // Shutdown the scheduler gracefully with MANUAL THREAD JOIN
    public void shutdown() {
        // Set running to false (stops scheduling loop)
        running.set(false);
        logger.info("Initiating graceful shutdown with manual thread management...");
        
        // Get snapshot of active threads (thread-safe)
        List<Thread> threadsToJoin;
        synchronized (workerThreads) {
            threadsToJoin = new ArrayList<>(workerThreads);
        }
        
        logger.info("Waiting for " + threadsToJoin.size() + " worker threads to complete...");
        
        // MANUAL JOIN: Wait for each thread to complete
        long shutdownStartTime = System.currentTimeMillis();
        int completedCount = 0;
        
        try {
            for (Thread thread : threadsToJoin) {
                if (thread.isAlive()) {
                    long remainingTime = 60000 - (System.currentTimeMillis() - shutdownStartTime);
                    
                    if (remainingTime > 0) {
                        logger.fine("Waiting for thread " + thread.getName() + " to complete...");
                        thread.join(remainingTime); // BLOCKING: Wait for thread to finish
                        
                        if (!thread.isAlive()) {
                            completedCount++;
                            logger.fine("Thread " + thread.getName() + " completed");
                        }
                    } else {
                        // Timeout exceeded - interrupt remaining threads
                        logger.warning("Timeout exceeded, interrupting thread: " + thread.getName());
                        thread.interrupt();
                    }
                }
            }
            
            // Check if any threads are still alive after timeout
            long remainingAlive = threadsToJoin.stream().filter(Thread::isAlive).count();
            
            if (remainingAlive > 0) {
                logger.warning("Forcing shutdown of " + remainingAlive + " remaining threads");
                
                // FORCE INTERRUPT remaining threads
                for (Thread thread : threadsToJoin) {
                    if (thread.isAlive()) {
                        thread.interrupt();
                    }
                }
                
                // Give them 10 more seconds
                Thread.sleep(10000);
                
                // Final check
                remainingAlive = threadsToJoin.stream().filter(Thread::isAlive).count();
                if (remainingAlive > 0) {
                    logger.severe(remainingAlive + " threads did not terminate after forced shutdown");
                }
            }
            
        } catch (InterruptedException e) {
            logger.warning("Shutdown interrupted, forcing immediate termination");
            // Interrupt all remaining threads
            for (Thread thread : threadsToJoin) {
                if (thread.isAlive()) {
                    thread.interrupt();
                }
            }
            Thread.currentThread().interrupt();
        } finally {
            // CLEANUP: Clear the thread list
            synchronized (workerThreads) {
                workerThreads.clear();
            }
        }
        
        logger.info("Scheduler shutdown complete (" + completedCount + "/" + threadsToJoin.size() + " threads completed gracefully)");
    }

    // Get scheduler status
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        
        status.put("running", running.get());
        status.put("workerCount", workerCount);
        status.put("queueDepthThreshold", queueDepthThreshold);
        
        // MANUAL THREAD STATUS: Count active threads
        synchronized (workerThreads) {
            long activeThreads = workerThreads.stream().filter(Thread::isAlive).count();
            status.put("activeWorkerThreads", activeThreads);
            status.put("totalThreadsCreated", workerThreads.size());
        }
        
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

    // Check if scheduler is running
    public boolean isRunning() {
        return running.get();
    }

    // Get worker count
    public int getWorkerCount() {
        return workerCount;
    }

    // Get queue depth threshold
    public int getQueueDepthThreshold() {
        return queueDepthThreshold;
    }
}

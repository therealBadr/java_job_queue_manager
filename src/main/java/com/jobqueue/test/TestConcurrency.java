package com.jobqueue.test;

import com.jobqueue.db.Database;
import com.jobqueue.db.JobRepository;
import com.jobqueue.engine.Scheduler;
import com.jobqueue.jobs.EmailJob;
import com.jobqueue.jobs.CleanupJob;
import com.jobqueue.jobs.ReportJob;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.management.ThreadInfo;

/**
 * Stress test for detecting race conditions and deadlocks under concurrent load
 */
public class TestConcurrency {
    
    private static final int NUM_THREADS = 20;
    private static final int JOBS_PER_THREAD = 50;
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failureCount = new AtomicInteger(0);
    
    public static void main(String[] args) {
        System.out.println("=== Concurrency & Race Condition Test ===\n");
        System.out.println("Starting stress test with " + NUM_THREADS + " concurrent threads");
        System.out.println("Each thread will submit " + JOBS_PER_THREAD + " jobs");
        System.out.println("Total expected jobs: " + (NUM_THREADS * JOBS_PER_THREAD) + "\n");
        
        Database database = null;
        Scheduler scheduler = null;
        Thread schedulerThread = null;
        
        try {
            // Initialize components
            database = new Database();
            database.initialize();
            JobRepository repository = new JobRepository(database);
            
            scheduler = new Scheduler(8, repository);
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
            
            Thread.sleep(1000); // Let scheduler start
            
            // Test 1: Concurrent job submissions
            System.out.println("Test 1: Concurrent job submission...");
            long startTime = System.currentTimeMillis();
            testConcurrentSubmissions(scheduler);
            long duration = System.currentTimeMillis() - startTime;
            
            System.out.println("✓ Test 1 completed in " + duration + "ms");
            System.out.println("  Successful: " + successCount.get());
            System.out.println("  Failed: " + failureCount.get());
            
            // Test 2: Concurrent reads and writes
            System.out.println("\nTest 2: Concurrent database operations...");
            startTime = System.currentTimeMillis();
            testConcurrentDatabaseOps(repository);
            duration = System.currentTimeMillis() - startTime;
            System.out.println("✓ Test 2 completed in " + duration + "ms");
            
            // Test 3: Concurrent metrics queries
            System.out.println("\nTest 3: Concurrent metrics queries...");
            startTime = System.currentTimeMillis();
            testConcurrentMetrics(repository);
            duration = System.currentTimeMillis() - startTime;
            System.out.println("✓ Test 3 completed in " + duration + "ms");
            
            // Test 4: Check for deadlocks by monitoring thread states
            System.out.println("\nTest 4: Checking for deadlocks...");
            boolean hasDeadlock = detectDeadlock();
            if (hasDeadlock) {
                System.err.println("✗ DEADLOCK DETECTED!");
            } else {
                System.out.println("✓ No deadlocks detected");
            }
            
            // Wait for jobs to process
            System.out.println("\nWaiting 5 seconds for jobs to process...");
            Thread.sleep(5000);
            
            // Final statistics
            int queueDepth = repository.getQueueDepth();
            System.out.println("\nFinal queue depth: " + queueDepth);
            
            if (queueDepth == 0) {
                System.out.println("✓ All jobs processed successfully");
            } else {
                System.out.println("⚠ " + queueDepth + " jobs still pending");
            }
            
            System.out.println("\n=== All concurrency tests completed ===");
            
        } catch (Exception e) {
            System.err.println("✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Cleanup
            if (scheduler != null) {
                scheduler.shutdown();
            }
            if (schedulerThread != null) {
                schedulerThread.interrupt();
            }
            if (database != null) {
                database.close();
            }
        }
    }
    
    private static void testConcurrentSubmissions(Scheduler scheduler) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < JOBS_PER_THREAD; j++) {
                        try {
                            // Vary job types
                            if (j % 3 == 0) {
                                EmailJob job = new EmailJob(
                                    "user" + threadId + "_" + j + "@test.com",
                                    "Test Subject " + j,
                                    (j % 10) + 1
                                );
                                scheduler.submitJob(job);
                            } else if (j % 3 == 1) {
                                CleanupJob job = new CleanupJob(
                                    "cleanup" + threadId + "_" + j,
                                    "/tmp/test" + j,
                                    (j % 5) + 1
                                );
                                scheduler.submitJob(job);
                            } else {
                                ReportJob job = new ReportJob(
                                    "report" + threadId + "_" + j,
                                    "Report " + j,
                                    (j % 8) + 2
                                );
                                scheduler.submitJob(job);
                            }
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            failureCount.incrementAndGet();
                            System.err.println("Thread " + threadId + " failed to submit job " + j + ": " + e.getMessage());
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Wait for all threads to complete
        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    
    private static void testConcurrentDatabaseOps(JobRepository repository) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);
        
        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 20; j++) {
                        // Mix of read and write operations
                        repository.getQueueDepth();
                        repository.getPendingJobs(5);
                        repository.getAllJobs();
                    }
                } catch (Exception e) {
                    System.err.println("Database operation failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    
    private static void testConcurrentMetrics(JobRepository repository) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(15);
        CountDownLatch latch = new CountDownLatch(15);
        
        for (int i = 0; i < 15; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 30; j++) {
                        repository.getMetrics();
                        Thread.sleep(10);
                    }
                } catch (Exception e) {
                    System.err.println("Metrics query failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
    
    private static boolean detectDeadlock() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.findDeadlockedThreads();
        
        if (threadIds != null && threadIds.length > 0) {
            ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);
            System.err.println("\nDEADLOCK DETECTED:");
            for (ThreadInfo threadInfo : threadInfos) {
                System.err.println("Thread: " + threadInfo.getThreadName());
                System.err.println("State: " + threadInfo.getThreadState());
                System.err.println("Lock: " + threadInfo.getLockName());
                System.err.println("Lock owner: " + threadInfo.getLockOwnerName());
            }
            return true;
        }
        
        return false;
    }
}

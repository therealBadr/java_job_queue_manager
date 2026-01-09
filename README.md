# job_queue_manager

**TIER 3** | **Estimated time:** 60-80 hours  
**Technologies:** Java, JDBC, Concurrency, Streams API | **Collaboration:** 2 students

---

## Table of Contents
- [Foreword](#foreword)
- [Introduction](#introduction)
- [General Instructions](#general-instructions)
- [Mandatory Part](#mandatory-part)
- [Bonus Part](#bonus-part)
- [Two-Student Collaboration](#two-student-collaboration)
- [Submission and Peer Evaluation](#submission-and-peer-evaluation)

---

## Foreword

In the world of modern software engineering, not everything happens instantly. Background tasks, scheduled operations, batch processing, email notifications, data cleanup operations—these are the invisible workhorses that keep systems running smoothly while users sleep. But how do we manage thousands of these tasks efficiently, reliably, and concurrently?

Welcome to the world of job queue systems. From Redis Queue to Celery, from AWS SQS to RabbitMQ, production systems worldwide rely on sophisticated task schedulers to manage asynchronous work. These systems must handle concurrency without race conditions, persist state across crashes, retry failed operations intelligently, and scale gracefully under load.

In this project, you will build a production-grade job queue manager from scratch. This is not a CRUD application with a pretty interface. This is systems programming. You will wrestle with thread pools, database transactions, graceful shutdowns, and concurrent state management. You will understand why "just throw more threads at it" is not a solution. You will learn why professional engineers obsess over queue depths, backpressure, and poison pills.

This is engineering at its core—building reliable, concurrent systems that work correctly under stress.

---

## Introduction

The **job_queue_manager** project challenges you to implement a multi-threaded task execution engine with database persistence, polymorphic job types, and functional stream-based analytics. You will design a system where jobs are submitted, persisted to a database, picked up by worker threads, executed concurrently, and tracked through their entire lifecycle.

### Learning Objectives:
- Master Java concurrency primitives (ExecutorService, BlockingQueue, synchronization)
- Implement robust JDBC interactions with transactions and prepared statements
- Design extensible systems using interfaces and polymorphism
- Apply the Java Streams API for meaningful data processing and analytics
- Handle graceful shutdown, crash recovery, and error scenarios
- Understand the architecture of production task schedulers

Your system must support multiple types of jobs (email sending, file cleanup, report generation), execute them concurrently using a thread pool, persist all state to a relational database, and provide real-time analytics using Java Streams. The system must survive crashes and resume work correctly upon restart.

---

## General Instructions

### Language and Tools
- **Language:** Java 11 or higher (Java 17 recommended)
- **Build Tool:** Maven or Gradle (your choice)
- **Database:** Any JDBC-compatible database (H2 for simplicity, PostgreSQL for production-like experience)
- **Required Java APIs:** `java.util.concurrent`, `java.sql`, `java.util.stream`

### Project Structure

```
src/
 ├── core/
 │    ├── Job.java              // Core job interface
 │    ├── JobStatus.java         // Job lifecycle enum
 │    └── JobContext.java        // Execution context
 ├── engine/
 │    ├── Worker.java            // Thread worker implementation
 │    ├── Scheduler.java         // Main scheduling engine
 │    └── JobExecutor.java       // Execution coordinator
 ├── db/
 │    ├── JobRepository.java     // Database operations
 │    └── Database.java          // Connection management
 ├── jobs/
 │    ├── EmailJob.java          // Concrete job type
 │    ├── CleanupJob.java        // Concrete job type
 │    └── ReportJob.java         // Concrete job type
 └── app/
      └── Main.java              // Application entry point
```

### Compilation and Execution

```bash
# With Maven
mvn clean compile
mvn exec:java -Dexec.mainClass="app.Main"

# With Gradle
./gradlew build
./gradlew run
```

### Submission
- Your Git repository must be named `job_queue_manager`
- Include a comprehensive README.md explaining architecture and usage
- Include a `schema.sql` file for database initialization
- Include example configuration files
- No compiled files (.class, .jar) in the repository

> ⚠️ **Critical Requirements:**
> - Both students must understand the ENTIRE codebase, not just their assigned sections
> - This is not a theoretical exercise—your system must demonstrate real concurrent execution
> - JDBC must be used for meaningful persistence, not checkbox compliance
> - Streams must perform actual data processing, not decorative operations
> - The evaluator will verify thread safety and crash recovery with extreme prejudice

---

## Mandatory Part

### I. Core Architecture

#### Job Interface Design

Create a `Job` interface that serves as the abstraction for all executable tasks. This interface must define the contract that all concrete job types will implement.

```java
public interface Job {
    String getId();
    String getType();
    void execute(JobContext context) throws Exception;
    String getPayload();
    int getPriority();
}
```

**Requirements:**
- Each job must have a unique identifier (consider UUID)
- The `type` field identifies the concrete job class for deserialization
- The `execute()` method contains the actual job logic
- The `payload` stores job-specific data (JSON string recommended)
- Priority is an integer where higher values = higher priority

#### JobStatus Lifecycle

Implement a `JobStatus` enum representing the complete lifecycle of a job:

```java
public enum JobStatus {
    PENDING,    // Job submitted, waiting for execution
    RUNNING,    // Currently being executed by a worker
    SUCCESS,    // Completed successfully
    FAILED      // Failed with error
}
```

**State Transition Diagram:**
```
[PENDING] ──pick──> [RUNNING] ──success──> [SUCCESS]
                       │
                       └──error──> [FAILED]
                       
Note: No transition from SUCCESS or FAILED back to other states
(bonus: retry logic may create new jobs instead)
```

#### JobContext

The `JobContext` class provides execution context and logging capabilities to jobs during execution:

```java
public class JobContext {
    private final String jobId;
    private final JobRepository repository;
    
    public void log(String level, String message);
    public void updateProgress(String status);
    // Additional helper methods
}
```

---

### II. Database Layer

#### Schema Requirements

Your database must implement the following schema with proper constraints:

```sql
CREATE TABLE jobs (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(100) NOT NULL,
    payload TEXT,
    priority INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    scheduled_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);

CREATE TABLE job_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(10) NOT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_scheduled ON jobs(scheduled_time);
CREATE INDEX idx_logs_job_id ON job_logs(job_id);
```

#### JDBC Implementation Requirements

**Mandatory JDBC Practices:**
- **Prepared Statements:** ALL database operations must use PreparedStatement, never string concatenation
- **Connection Management:** Properly close connections, statements, and result sets (use try-with-resources)
- **Transaction Management:** Use transactions for multi-statement operations
- **SQL Injection Prevention:** Never construct SQL with user input strings

**Example: Proper JDBC usage**

```java
public void submitJob(Job job) throws SQLException {
    String sql = "INSERT INTO jobs (id, name, type, payload, " +
                 "priority, status) VALUES (?, ?, ?, ?, ?, ?)";
    
    try (Connection conn = database.getConnection();
         PreparedStatement stmt = conn.prepareStatement(sql)) {
        
        stmt.setString(1, job.getId());
        stmt.setString(2, job.getType());
        stmt.setString(3, job.getType());
        stmt.setString(4, job.getPayload());
        stmt.setInt(5, job.getPriority());
        stmt.setString(6, JobStatus.PENDING.name());
        
        stmt.executeUpdate();
    }
}
```

#### Crash Recovery Mechanism

Your system must implement crash recovery. When the scheduler starts, it must:

1. Query for all jobs in `RUNNING` status
2. Reset these jobs to `PENDING` status (they were interrupted)
3. Log the recovery operation
4. Resume normal operations

> **Why this matters:** In production, processes crash. Servers restart. Your system must not lose track of work or leave jobs in inconsistent states. This recovery mechanism is what separates toy projects from production-grade systems.

---

### III. Job Execution Engine

#### Thread Pool Architecture

Implement a worker pool using Java's `ExecutorService`. The number of worker threads should be configurable but reasonable (recommend 4-10).

```java
public class Scheduler {
    private final ExecutorService executorService;
    private final JobRepository repository;
    private final AtomicBoolean running;
    
    public Scheduler(int workerCount, JobRepository repository) {
        this.executorService = Executors.newFixedThreadPool(workerCount);
        this.repository = repository;
        this.running = new AtomicBoolean(false);
    }
    
    public void start() {
        running.set(true);
        // Main scheduling loop
    }
    
    public void shutdown() {
        running.set(false);
        executorService.shutdown();
        // Graceful shutdown logic
    }
}
```

#### Job Picking Algorithm

Implement a loop that continuously:

1. Queries the database for jobs in PENDING status
2. Orders by priority (DESC) then scheduled_time (ASC)
3. Attempts to acquire a job (atomically update status to RUNNING)
4. Submits the job to the thread pool
5. Sleeps briefly if no jobs are available

> ⚠️ **Concurrency Challenge:** Multiple scheduler instances (or threads) might try to pick the same job. Your UPDATE statement must be atomic. Consider using database-level locking or a WHERE clause that checks the current status:
> ```sql
> UPDATE jobs SET status = 'RUNNING', started_at = ? 
> WHERE id = ? AND status = 'PENDING'
> ```
> Check the update count to verify you successfully claimed the job.

#### Worker Implementation

Each worker thread executes jobs submitted by the scheduler:

```java
public class Worker implements Runnable {
    private final Job job;
    private final JobRepository repository;
    
    public void run() {
        JobContext context = new JobContext(job.getId(), repository);
        try {
            job.execute(context);
            repository.updateJobStatus(
                job.getId(), 
                JobStatus.SUCCESS, 
                null
            );
        } catch (Exception e) {
            repository.updateJobStatus(
                job.getId(), 
                JobStatus.FAILED, 
                e.getMessage()
            );
            context.log("ERROR", "Job failed: " + e.getMessage());
        }
    }
}
```

#### Graceful Shutdown

Implement proper shutdown that:
- Stops accepting new jobs
- Waits for currently executing jobs to complete (with timeout)
- Forces termination if timeout exceeded
- Logs all shutdown activities

```java
public void shutdown() {
    running.set(false);
    executorService.shutdown();
    
    try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                System.err.println("Pool did not terminate");
            }
        }
    } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
    }
}
```

---

### IV. Concrete Job Implementations

Implement at least three different job types demonstrating various use cases:

#### 1. EmailJob

```java
public class EmailJob implements Job {
    // Simulates sending an email
    // Payload: {"to": "user@example.com", "subject": "...", "body": "..."}
    // Should parse JSON, simulate network delay (Thread.sleep)
    // Log each step via JobContext
}
```

#### 2. FileCleanupJob

```java
public class FileCleanupJob implements Job {
    // Simulates cleaning up old files
    // Payload: {"directory": "/tmp", "olderThanDays": 30}
    // Can simulate file operations or actually perform them
    // Should handle file I/O exceptions gracefully
}
```

#### 3. ReportGenerationJob

```java
public class ReportGenerationJob implements Job {
    // Simulates generating a business report
    // Payload: {"reportType": "sales", "startDate": "...", "endDate": "..."}
    // Should demonstrate longer-running task
    // Update progress through JobContext
}
```

**Implementation Notes:**
- Each job must properly parse its JSON payload
- Use proper exception handling
- Log significant actions via JobContext
- Demonstrate polymorphic behavior (all executed through Job interface)
- Consider edge cases and validation

---

### V. Streams API Integration

Implement meaningful data processing using Java Streams. This is not optional decoration—these operations must provide real value.

#### Required Stream Operations

```java
// 1. Filter jobs by status
public List<Job> getJobsByStatus(JobStatus status) {
    return repository.getAllJobs().stream()
        .filter(job -> job.getStatus() == status)
        .collect(Collectors.toList());
}

// 2. Group jobs by type with counts
public Map<String, Long> getJobCountsByType() {
    return repository.getAllJobs().stream()
        .collect(Collectors.groupingBy(
            Job::getType,
            Collectors.counting()
        ));
}

// 3. Calculate average execution time by job type
public Map<String, Double> getAverageExecutionTimeByType() {
    return repository.getCompletedJobs().stream()
        .filter(job -> job.getStartedAt() != null && 
                       job.getCompletedAt() != null)
        .collect(Collectors.groupingBy(
            Job::getType,
            Collectors.averagingLong(job -> 
                job.getCompletedAt().getTime() - 
                job.getStartedAt().getTime()
            )
        ));
}

// 4. Find top N highest priority pending jobs
public List<Job> getTopPriorityPendingJobs(int limit) {
    return repository.getAllJobs().stream()
        .filter(job -> job.getStatus() == JobStatus.PENDING)
        .sorted(Comparator.comparingInt(Job::getPriority).reversed())
        .limit(limit)
        .collect(Collectors.toList());
}
```

> ⚠️ **Stream Quality Standards:**
> - Do NOT use streams for simple iterations that could be for-loops
> - Each stream operation must process real data and return meaningful results
> - Demonstrate understanding of filter, map, reduce, collect, groupingBy, and sorting
> - At least one operation must involve multiple intermediate operations
> - Consider performance implications for large datasets

---

## Bonus Part

The bonus section is only evaluated if the mandatory part is PERFECT. Do not attempt bonuses if you have any doubts about the core functionality.

### Priority-Based Scheduling
Enhance the job picking algorithm to respect job priorities more intelligently. Higher priority jobs should be picked first, but avoid starvation of lower priority jobs.

### Retry Mechanism with Exponential Backoff
Implement automatic retry for failed jobs:
- Add `max_retries` and `retry_count` fields
- When a job fails, check if retries remain
- Schedule retry with exponential backoff (delay = 2^retry_count minutes)
- After max retries, mark as permanently failed

### Backpressure Handling
Implement queue depth monitoring. When pending jobs exceed a threshold:
- Log warnings
- Reject new job submissions (throw exception)
- Provide API to check current queue depth

### Job Cancellation Support
Allow jobs to be cancelled:
- Add CANCELLED status
- Provide cancellation API
- Check cancellation flag during long-running jobs
- Clean up resources properly on cancellation

### Monitoring Metrics Endpoint
Create a simple HTTP endpoint (using Java's built-in HttpServer) that returns:
- Total jobs processed
- Current queue depth
- Success/failure rates
- Average execution time
- Active worker count

### Dead Letter Queue
Move permanently failed jobs to a separate table for manual inspection:
- Create `dead_letter_queue` table
- Move jobs that exhaust all retries
- Include original error messages and context
- Provide mechanism to replay jobs from DLQ

---

## Two-Student Collaboration

> 💡 **Critical Rule:** Both students must understand the ENTIRE codebase. During defense, the evaluator will ask BOTH students detailed questions about ALL parts of the system, not just their assigned section. If one student cannot explain the other's code, the project fails.

### Suggested Division of Labor

#### Student A - Concurrency & Execution Engine
- Implement the Scheduler class and main scheduling loop
- Implement Worker threads and ExecutorService management
- Implement graceful shutdown mechanism
- Implement crash recovery on startup
- Handle thread synchronization and atomic operations
- Implement Main class and application initialization

#### Student B - Persistence & Job Types
- Design and implement database schema
- Implement JobRepository with all JDBC operations
- Implement Database connection management
- Implement all three concrete job types
- Implement JobContext and logging functionality
- Implement Streams-based analytics functions

#### Shared Responsibilities
- Design Job interface and JobStatus enum together
- Integration testing and debugging together
- Documentation and README together
- Defense preparation - each must understand the other's code completely

### Defense Preparation

Both students must be prepared to answer:
- **Architecture:** "Walk me through the entire system flow, from job submission to completion"
- **Concurrency:** "What happens if two workers try to pick the same job simultaneously?"
- **JDBC:** "Why must we use PreparedStatement? Show me where you use transactions"
- **Crash Recovery:** "Show me how the system recovers from a crash mid-execution"
- **Streams:** "Explain each stream operation—why streams and not loops?"
- **Interfaces:** "Why use an interface for Job? What if we want to add a new job type?"
- **Performance:** "What happens under high load? How many jobs can you handle?"
- **Edge Cases:** "What if the database goes down? What if a job runs forever?"

---

## Submission and Peer Evaluation

### Repository Structure

```
job_queue_manager/
├── src/
│   └── [your package structure]
├── schema.sql
├── pom.xml (or build.gradle)
├── README.md
├── .gitignore
└── docs/
    ├── architecture.md
    └── defense-guide.md
```

### README Requirements

Your README.md must include:
- Project description and architecture overview
- Technologies used and why
- Database setup instructions
- Build and run instructions
- Example usage scenarios
- Known limitations and future improvements
- Division of labor between team members

### Evaluation Criteria

| Category | Points | Criteria |
|----------|--------|----------|
| **Core Architecture** | 20 | Job interface, JobStatus enum, JobContext properly designed and implemented |
| **Database Layer** | 20 | Schema correct, JDBC with prepared statements, transactions, crash recovery works |
| **Execution Engine** | 25 | Thread pool, job picking, worker execution, graceful shutdown all functional |
| **Job Implementations** | 15 | Three distinct job types, proper polymorphism, error handling |
| **Streams API** | 15 | Meaningful stream operations, proper functional programming usage |
| **Code Quality** | 5 | Clean code, proper separation of concerns, readable and maintainable |
| **Defense** | Bonus/Penalty | Both students must demonstrate complete understanding |

**Total:** 100 points

### Common Failure Scenarios
- One student cannot explain the other's code: **FAIL**
- Race conditions in job picking: **-15 points**
- No crash recovery or doesn't work: **-10 points**
- Decorative stream usage (could be simple loops): **-10 points**
- SQL injection vulnerabilities (no prepared statements): **-15 points**
- System doesn't actually run concurrently: **FAIL**
- Cannot demonstrate the system working: **FAIL**

---

## Final Notes

This project is about building real, production-grade concurrent systems. Every line of code you write should demonstrate understanding of:
- Why concurrency is hard
- Why transactions matter
- Why interfaces enable extensibility
- Why proper shutdown prevents data loss

You are not building a toy. You are building a system that could actually be deployed. Treat it with that level of seriousness.

Good luck, and may your threads never deadlock. 🚀

---

*This subject follows the 42 school philosophy: challenge yourself, understand deeply, build something real.*
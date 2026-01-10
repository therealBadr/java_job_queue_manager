# Job Queue Manager - Production-Grade Task Scheduler

A robust, production-ready job queue system built with Java 17, featuring multi-threaded execution, crash recovery, priority scheduling, and comprehensive monitoring capabilities.

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/projects/jdk/17/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-blue.svg)](https://maven.apache.org/)
[![H2 Database](https://img.shields.io/badge/H2-2.2.224-green.svg)](https://www.h2database.com/)

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Key Features](#key-features)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Running the Application](#running-the-application)
- [Project Structure](#project-structure)
- [Design Decisions](#design-decisions)
- [API Endpoints](#api-endpoints)
- [Testing](#testing)
- [Database Schema](#database-schema)
- [Streams API Usage](#streams-api-usage)
- [Future Enhancements](#future-enhancements)

---

## Architecture Overview

The Job Queue Manager implements a producer-consumer pattern with persistent storage, ensuring reliable task execution even in the face of system failures.

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Application Layer                        │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐   │
│  │   Main   │  │ DLQ Replay   │  │  Cancellation Demo     │   │
│  └────┬─────┘  └──────┬───────┘  └───────────┬────────────┘   │
└───────┼────────────────┼──────────────────────┼─────────────────┘
        │                │                      │
┌───────┼────────────────┼──────────────────────┼─────────────────┐
│       │           Scheduling Layer           │                  │
│  ┌────▼─────┐                          ┌─────▼─────┐           │
│  │Scheduler │◄────────────────────────►│  Metrics  │           │
│  │          │   submits/monitors       │  Server   │           │
│  └────┬─────┘                          └───────────┘           │
│       │                                                          │
│  ┌────▼─────────────────────────────────────┐                  │
│  │        Worker Thread Pool (8)             │                  │
│  │  ┌────────┐ ┌────────┐ ┌────────┐       │                  │
│  │  │Worker 1│ │Worker 2│ │Worker N│  ...  │                  │
│  │  └────┬───┘ └────┬───┘ └────┬───┘       │                  │
│  └───────┼──────────┼──────────┼────────────┘                  │
└──────────┼──────────┼──────────┼─────────────────────────────┘
           │          │          │
┌──────────┼──────────┼──────────┼─────────────────────────────┐
│          │    Persistence Layer │                             │
│     ┌────▼──────────▼──────────▼──────┐                       │
│     │      JobRepository               │                       │
│     │  - submitJob()                   │                       │
│     │  - claimJob() [ATOMIC]           │                       │
│     │  - updateJobStatus()             │                       │
│     │  - getPendingJobs()              │                       │
│     │  - moveToDeadLetterQueue()       │                       │
│     └────────────┬─────────────────────┘                       │
│                  │                                              │
│     ┌────────────▼─────────────────────┐                       │
│     │   Database Connection Pool       │                       │
│     │   (10 connections, H2 in-memory) │                       │
│     └────────────┬─────────────────────┘                       │
│                  │                                              │
│     ┌────────────▼─────────────────────┐                       │
│     │          H2 Database              │                       │
│     │  ┌────────┐  ┌──────────────┐   │                       │
│     │  │ jobs   │  │ job_logs     │   │                       │
│     │  └────────┘  └──────────────┘   │                       │
│     │  ┌─────────────────────────┐    │                       │
│     │  │  dead_letter_queue      │    │                       │
│     │  └─────────────────────────┘    │                       │
│     └──────────────────────────────────┘                       │
└────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         Job Types Layer                          │
│  ┌──────────┐  ┌────────────┐  ┌─────────────┐ ┌────────────┐ │
│  │EmailJob  │  │CleanupJob  │  │ReportJob    │ │FailingJob  │ │
│  └──────────┘  └────────────┘  └─────────────┘ └────────────┘ │
│           All implement: Job interface                          │
└─────────────────────────────────────────────────────────────────┘
```

### Concurrency Model

The system employs a **fixed thread pool** pattern with **atomic job claiming** to ensure thread-safe execution:

1. **Scheduler Thread**: Single dedicated thread polls the database for pending jobs every second
2. **Worker Pool**: Fixed pool of 8 worker threads execute jobs concurrently
3. **Atomic Claiming**: Database-level locking prevents multiple workers from claiming the same job
4. **Connection Pool**: 10 database connections shared across all threads
5. **Non-blocking Operations**: Workers execute independently without blocking the scheduler

**Key Thread Safety Mechanisms**:
- `AtomicBoolean` for state flags (running, backpressure)
- Database transactions for job state transitions
- Optimistic locking via `UPDATE ... WHERE status = 'PENDING'`
- No shared mutable state between workers

---

## Key Features

### Mandatory Features ✅

#### 1. **Multi-threaded Execution with Configurable Worker Pool**
```java
Scheduler scheduler = new Scheduler(8, repository); // 8 workers
```
- Fixed thread pool using `Executors.newFixedThreadPool()`
- Configurable pool size at initialization
- Independent worker threads with separate execution contexts

#### 2. **JDBC-based Persistence with H2 Database**
- Connection pooling (10 connections)
- In-memory H2 database for fast operations
- Transactional integrity for state changes
- Automatic schema initialization

#### 3. **Polymorphic Job Types**
- **EmailJob**: Send emails with retry support
- **CleanupJob**: File system cleanup operations
- **ReportJob**: Generate reports with cancellation checkpoints
- **FailingJob**: Test job for DLQ demonstration
- All implement common `Job` interface

#### 4. **Crash Recovery Mechanism**
```java
int recovered = repository.recoverCrashedJobs();
```
- Automatically detects jobs stuck in `RUNNING` state
- Resets them to `PENDING` on scheduler startup
- Prevents job loss due to unexpected shutdowns

#### 5. **Java Streams API Analytics**
```java
analyticsService.getJobCountsByType()
    .entrySet().stream()
    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
    .forEach(entry -> display(entry));
```
- Job statistics aggregation
- Performance metrics calculation
- Failure rate analysis
- Type-based job distribution

#### 6. **Graceful Shutdown**
```java
scheduler.shutdown(); // Waits up to 60 seconds for completion
```
- Stops accepting new jobs
- Waits for in-flight jobs to complete (60s timeout)
- Closes database connections cleanly
- Shutdown hook for SIGTERM/SIGINT handling

---

### Bonus Features Implemented 🚀

#### 1. **Priority-based Scheduling with Anti-Starvation**
- Jobs assigned priority levels (1-10)
- **Anti-starvation algorithm**: `effective_priority = base_priority + (age_in_minutes / 60)`
- Old low-priority jobs gain +1 priority per hour waiting
- Prevents indefinite postponement of low-priority tasks

#### 2. **Exponential Backoff Retry Mechanism**
```java
long delayMinutes = (long) Math.pow(2, retryCount);
// Retry 1: 2 minutes, Retry 2: 4 minutes, Retry 3: 8 minutes
```
- Configurable `maxRetries` per job type
- Automatic retry scheduling after failure
- Exponential delay prevents resource exhaustion
- Jobs moved to DLQ after exhausting retries

#### 3. **Backpressure Handling**
- Monitors queue depth in real-time
- Threshold: 1000 pending jobs (configurable)
- When exceeded:
  - Rejects new job submissions with `QueueOverloadException`
  - Logs warning with current queue depth
- Releases when depth drops below 80% of threshold
- Prevents system overload and OOM errors

#### 4. **Job Cancellation Support**
```java
boolean cancelled = scheduler.cancelJob(jobId);
```
- Cancel jobs in `PENDING` or `RUNNING` state
- Jobs check cancellation at strategic checkpoints via `context.throwIfCancelled()`
- Status updated to `CANCELLED` in database
- Workers catch `CancellationException` and handle gracefully

#### 5. **HTTP Metrics Endpoint**
```bash
curl http://localhost:8080/metrics
```
**Response**:
```json
{
  "total_jobs_processed": 150,
  "pending_jobs": 5,
  "running_jobs": 3,
  "success_rate": 94.5,
  "failure_rate": 5.5,
  "average_execution_time_ms": 2341.7,
  "queue_depth": 12,
  "active_workers": 8,
  "uptime_seconds": 3600
}
```
- Real-time system metrics via HTTP
- Uses `com.sun.net.httpserver.HttpServer` (no external dependencies)
- JSON response format
- Metrics updated dynamically

#### 6. **Dead Letter Queue (DLQ)**
- Failed jobs automatically moved to DLQ after exhausting retries
- Query DLQ contents: `repository.getDLQJobs(limit)`
- Replay jobs from DLQ: `repository.replayFromDLQ(jobId)`
- Clear DLQ: `repository.clearDLQ()`
- Transaction-based operations ensure data integrity
- Preserves original error messages for debugging

---

## Prerequisites

- **Java 17 or higher**
  ```bash
  java -version  # Should show 17+
  ```

- **Maven 3.6+**
  ```bash
  mvn -version   # Should show 3.6.0+
  ```

- **No external database required** - H2 is embedded and auto-configured

---

## Setup Instructions

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd queue
   ```

2. **Build the project**
   ```bash
   mvn clean install
   ```
   This will:
   - Compile all source files (30 classes)
   - Run unit tests (if present)
   - Package the application
   - Download dependencies (H2, etc.)

3. **Database auto-initialization**
   - Database schema is created automatically on first run
   - Located in: `src/main/resources/schema.sql`
   - No manual setup required

---

## Running the Application

### Main Application

Run the full job queue system with demo jobs:

```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.app.Main"
```

**What it does**:
1. Initializes database and connection pool
2. Recovers any crashed jobs from previous run
3. Starts scheduler with 8 workers
4. Submits 100 demo jobs (50 Email, 30 Cleanup, 20 Report)
5. Starts HTTP metrics server on port 8080
6. Displays comprehensive analytics when complete
7. Graceful shutdown on Ctrl+C

**Expected output**:
```
Initializing database...
Database initialized successfully
Scheduler started with 8 workers
Submitting 100 demo jobs...
✓ All jobs submitted
Metrics server started on port 8080
...
[Jobs execute]
...
Analytics Report:
╔════════════════════════════════════════════════╗
║            Job Statistics                      ║
╠════════════════════════════════════════════════╣
║  Total Jobs: 100                               ║
║  Succeeded:  98                                ║
║  Failed:     2                                 ║
║  Success Rate: 98.00%                          ║
╚════════════════════════════════════════════════╝
```

### Demo Applications

#### Cancellation Demo

Test job cancellation functionality:

```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.app.CancellationDemo"
```

**Demonstrates**:
- Submits 10 long-running ReportJobs
- Cancels 5 jobs mid-execution
- Verifies cancellation works correctly
- Shows which jobs were cancelled vs. completed

#### DLQ Replay Demo

Test Dead Letter Queue lifecycle:

```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.app.DLQReplayDemo"
```

**Demonstrates**:
- Submits 20 jobs that fail by design
- All jobs exhaust retries (maxRetries=2) and move to DLQ
- Displays DLQ contents
- Selects 5 jobs for replay
- "Fixes" the jobs and replays them
- Verifies successful execution after replay
- Shows DLQ statistics

---

## Project Structure

```
queue/
├── src/main/java/com/jobqueue/
│   ├── app/                          # Application entry points
│   │   ├── Main.java                 # Main application with analytics
│   │   ├── CancellationDemo.java    # Job cancellation demonstration
│   │   ├── DLQReplayDemo.java       # DLQ lifecycle demonstration
│   │   └── MetricsServer.java       # HTTP metrics endpoint server
│   │
│   ├── core/                         # Core interfaces and base classes
│   │   ├── Job.java                  # Job interface
│   │   ├── JobContext.java          # Execution context for jobs
│   │   ├── JobStatus.java           # Job state enum
│   │   ├── BaseJob.java             # Abstract base implementation
│   │   ├── CancellationException.java
│   │   └── QueueOverloadException.java
│   │
│   ├── jobs/                         # Concrete job implementations
│   │   ├── EmailJob.java            # Email sending job
│   │   ├── CleanupJob.java          # File cleanup job
│   │   ├── ReportJob.java           # Report generation job
│   │   └── FailingJob.java          # Test job that fails by design
│   │
│   ├── engine/                       # Execution engine
│   │   ├── Scheduler.java           # Main scheduler (anti-starvation)
│   │   └── Worker.java              # Worker thread (retry logic)
│   │
│   ├── db/                           # Database layer
│   │   ├── Database.java            # Connection pool manager
│   │   └── JobRepository.java       # JDBC data access (DLQ support)
│   │
│   └── analytics/                    # Analytics and reporting
│       └── AnalyticsService.java    # Streams API analytics
│
├── src/main/resources/
│   └── schema.sql                    # Database schema definition
│
├── src/test/java/com/jobqueue/test/ # Test classes
│   ├── TestCore.java                # Core functionality tests
│   ├── TestWorker.java              # Worker thread tests
│   ├── TestScheduler.java           # Scheduler tests
│   ├── TestAnalytics.java           # Analytics tests
│   ├── TestIntegration.java         # End-to-end tests
│   ├── TestDLQ.java                 # DLQ functionality tests
│   ├── TestMetricsServer.java       # HTTP server tests
│   ├── TestBackpressure.java        # Backpressure tests
│   ├── TestMainStartup.java         # Application startup tests
│   └── TestConcurrency.java         # Concurrency/deadlock tests
│
├── pom.xml                           # Maven configuration
└── README.md                         # This file
```

### Key Packages

- **app**: Entry points and demo applications
- **core**: Fundamental interfaces and exceptions
- **jobs**: Polymorphic job type implementations
- **engine**: Scheduler and worker execution logic
- **db**: JDBC persistence layer with repository pattern
- **analytics**: Java Streams API for statistics

---

## Design Decisions

### Thread Pool Size: 8 Workers (Configurable)

**Rationale**:
- Balances CPU utilization with context switching overhead
- Suitable for I/O-bound tasks (database, network, file operations)
- Can be adjusted based on workload characteristics:
  ```java
  Scheduler scheduler = new Scheduler(16, repository); // 16 workers
  ```

**Trade-offs**:
- More workers = higher throughput but more memory/context switches
- Fewer workers = lower overhead but reduced parallelism

---

### Atomic Job Claiming Prevents Duplicate Execution

**Implementation**:
```java
UPDATE jobs 
SET status = 'RUNNING', started_at = NOW()
WHERE id = ? AND status = 'PENDING'
```

**Benefits**:
- Database-level atomicity ensures only one worker claims a job
- No distributed locks needed
- Works correctly even with multiple scheduler instances (future enhancement)

**Alternative considered**: Application-level locking
- Rejected due to complexity and single-point-of-failure

---

### Exponential Backoff: 2^n Minutes Delay

**Formula**: `delay = 2^retryCount minutes`

**Progression**:
- Retry 1: 2 minutes (120 seconds)
- Retry 2: 4 minutes (240 seconds)
- Retry 3: 8 minutes (480 seconds)
- Retry 4: 16 minutes (960 seconds)

**Rationale**:
- Gives transient failures time to resolve (network glitches, temporary service outages)
- Prevents retry storms that could worsen the problem
- Industry-standard approach used by AWS SQS, Azure Service Bus, etc.

**Alternative considered**: Fixed delay
- Rejected because it can overwhelm failing services

---

### Backpressure Threshold: 1000 Pending Jobs

**Trigger**: Queue depth > 1000
**Release**: Queue depth < 800 (80% of threshold)

**Rationale**:
- 1000 jobs is ~2 minutes of work for 8 workers (assuming 1 second per job)
- Prevents memory exhaustion from unbounded queue growth
- 80% release threshold prevents oscillation (hysteresis)

**Monitoring**:
```java
if (queueDepth > 1000) {
    backpressureActive.set(true);
    logger.warning("Backpressure activated: " + queueDepth + " pending jobs");
}
```

---

### Transaction Isolation for DLQ Operations

**Implementation**:
```java
connection.setAutoCommit(false);
try {
    // 1. Query job from jobs table
    // 2. Insert into dead_letter_queue
    // 3. Delete from jobs table
    connection.commit();
} catch (Exception e) {
    connection.rollback();
    throw e;
}
```

**Benefits**:
- ACID guarantees: Job is never lost or duplicated
- All-or-nothing semantics for DLQ moves
- Consistent state even if database crashes mid-operation

---

### Connection Pool Size: 10 Connections

**Rationale**:
- Scheduler thread: 1 connection
- Worker threads: 8 connections (1 per worker)
- Metrics server: 1 connection
- Total: 10 connections

**Trade-offs**:
- H2 in-memory database can handle hundreds of connections
- 10 is conservative to prevent connection exhaustion
- Can be increased if adding more workers

---

## API Endpoints

### GET http://localhost:8080/metrics

**Description**: Returns real-time job queue metrics in JSON format

**Response Schema**:
```json
{
  "total_jobs_processed": <number>,   // Total completed jobs (success + failed)
  "pending_jobs": <number>,            // Jobs waiting to execute
  "running_jobs": <number>,            // Currently executing jobs
  "success_rate": <float>,             // Percentage of successful jobs
  "failure_rate": <float>,             // Percentage of failed jobs
  "average_execution_time_ms": <float>, // Mean execution time
  "queue_depth": <number>,             // Pending + scheduled jobs
  "active_workers": <number>,          // Worker pool size
  "uptime_seconds": <number>           // Server uptime
}
```

**Example**:
```bash
curl http://localhost:8080/metrics
```

**Response**:
```json
{
  "total_jobs_processed": 245,
  "pending_jobs": 12,
  "running_jobs": 5,
  "success_rate": 96.73,
  "failure_rate": 3.27,
  "average_execution_time_ms": 1847.32,
  "queue_depth": 17,
  "active_workers": 8,
  "uptime_seconds": 3847
}
```

**Use Cases**:
- Monitoring dashboards (Grafana, Datadog)
- Health checks in orchestration systems (Kubernetes)
- Performance analysis and capacity planning

---

## Testing

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TestCore
mvn test -Dtest=TestConcurrency
```

### Test Suite Overview

| Test Class | Purpose | Tests |
|------------|---------|-------|
| **TestCore** | Core job functionality | 6 |
| **TestWorker** | Worker thread behavior | 5 |
| **TestScheduler** | Scheduler anti-starvation | 4 |
| **TestAnalytics** | Streams API analytics | 5 |
| **TestIntegration** | End-to-end workflow | 4 |
| **TestDLQ** | Dead Letter Queue | 5 |
| **TestMetricsServer** | HTTP endpoint | 5 |
| **TestBackpressure** | Queue overload handling | 3 |
| **TestMainStartup** | Application initialization | 6 |
| **TestConcurrency** | Deadlock detection | 4 |
| **Total** | | **47 tests** |

### Demo Scenarios

#### 1. **Basic Execution**
```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.app.Main"
```
- Tests normal job execution flow
- Validates all job types (Email, Cleanup, Report)
- Demonstrates analytics output

#### 2. **Cancellation Testing**
```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.app.CancellationDemo"
```
- Submits 10 long-running jobs
- Cancels 5 mid-execution
- Verifies cancellation mechanism works correctly

#### 3. **DLQ Lifecycle**
```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.app.DLQReplayDemo"
```
- 20 jobs fail and move to DLQ
- Replay 5 jobs after "fixing" them
- Demonstrates full DLQ workflow

#### 4. **Concurrency Stress Test**
```bash
mvn exec:java -Dexec.mainClass="com.jobqueue.test.TestConcurrency"
```
- 20 threads × 50 jobs = 1000 concurrent submissions
- Tests for deadlocks and race conditions
- Validates thread safety

---

## Database Schema

### Tables

#### 1. **jobs** - Main job queue table

```sql
CREATE TABLE jobs (
    id VARCHAR(36) PRIMARY KEY,           -- UUID
    name VARCHAR(255) NOT NULL,           -- Job name
    type VARCHAR(50) NOT NULL,            -- Job class name
    payload TEXT,                         -- JSON payload
    priority INT DEFAULT 0,               -- Priority (1-10)
    status VARCHAR(20) NOT NULL,          -- PENDING/RUNNING/SUCCESS/FAILED/CANCELLED
    scheduled_time TIMESTAMP NOT NULL,    -- When to execute
    created_at TIMESTAMP NOT NULL,        -- Submission time
    started_at TIMESTAMP,                 -- Execution start time
    completed_at TIMESTAMP,               -- Execution end time
    error_message TEXT,                   -- Error details if failed
    retry_count INT DEFAULT 0,            -- Current retry attempt
    max_retries INT DEFAULT 3             -- Maximum retry attempts
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_scheduled_time ON jobs(scheduled_time);
CREATE INDEX idx_jobs_priority ON jobs(priority);
```

#### 2. **job_logs** - Execution logs

```sql
CREATE TABLE job_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,         -- Foreign key to jobs.id
    timestamp TIMESTAMP NOT NULL,         -- Log timestamp
    level VARCHAR(10) NOT NULL,           -- INFO/WARN/ERROR
    message TEXT NOT NULL,                -- Log message
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX idx_logs_job_id ON job_logs(job_id);
CREATE INDEX idx_logs_timestamp ON job_logs(timestamp);
```

#### 3. **dead_letter_queue** - Failed jobs

```sql
CREATE TABLE dead_letter_queue (
    id VARCHAR(36) PRIMARY KEY,           -- Original job ID
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    payload TEXT,
    priority INT,
    original_error TEXT,                  -- Error that caused failure
    created_at TIMESTAMP NOT NULL,        -- Original creation time
    failed_at TIMESTAMP NOT NULL,         -- When it failed
    retry_count INT,                      -- Final retry count
    moved_at TIMESTAMP NOT NULL           -- When moved to DLQ
);

CREATE INDEX idx_dlq_moved_at ON dead_letter_queue(moved_at);
CREATE INDEX idx_dlq_type ON dead_letter_queue(type);
```

### Relationships

```
jobs (1) ───< (N) job_logs
             └──────┘
        (FK: job_id references jobs.id)

jobs ──[exhausted retries]──> dead_letter_queue
    (Manual move, no FK constraint)
```

---

## Streams API Usage

The project extensively uses **Java Streams API** for analytics and data processing. Here are all stream operations with examples:

### 1. **Job Counts by Type**

**Location**: `AnalyticsService.getJobCountsByType()`

```java
Map<String, Long> jobCountsByType = getAllJobs().stream()
    .collect(Collectors.groupingBy(
        JobRepository.JobData::getType,
        Collectors.counting()
    ));
```

**Operations**:
- `stream()` - Create stream from job list
- `groupingBy()` - Group jobs by type (EmailJob, CleanupJob, etc.)
- `counting()` - Count jobs in each group

**Output**:
```
EmailJob: 50
CleanupJob: 30
ReportJob: 20
```

---

### 2. **Job Counts by Status**

**Location**: `AnalyticsService.getJobCountsByStatus()`

```java
Map<JobStatus, Long> jobCountsByStatus = getAllJobs().stream()
    .collect(Collectors.groupingBy(
        JobRepository.JobData::getStatus,
        Collectors.counting()
    ));
```

**Operations**:
- `stream()` - Create stream
- `groupingBy()` - Group by status (SUCCESS, FAILED, CANCELLED, etc.)
- `counting()` - Count jobs in each status

**Output**:
```
SUCCESS: 95
FAILED: 3
CANCELLED: 2
PENDING: 0
```

---

### 3. **Average Execution Time**

**Location**: `AnalyticsService.getAverageExecutionTime()`

```java
OptionalDouble avgTime = getAllJobs().stream()
    .filter(job -> job.getStatus().isTerminal())
    .filter(job -> job.getStartedAt() != null && job.getCompletedAt() != null)
    .mapToLong(job -> 
        Duration.between(job.getStartedAt(), job.getCompletedAt()).toMillis()
    )
    .average();
```

**Operations**:
- `stream()` - Create stream
- `filter()` - Only terminal states (SUCCESS/FAILED/CANCELLED)
- `filter()` - Only jobs with timing data
- `mapToLong()` - Extract duration in milliseconds
- `average()` - Calculate mean execution time

**Output**: `2341.5 ms`

---

### 4. **Top Priority Jobs**

**Location**: `AnalyticsService.getTopPriorityJobs()`

```java
List<JobRepository.JobData> topJobs = getAllJobs().stream()
    .sorted(Comparator.comparingInt(JobRepository.JobData::getPriority).reversed())
    .limit(10)
    .collect(Collectors.toList());
```

**Operations**:
- `stream()` - Create stream
- `sorted()` - Sort by priority (highest first)
- `reversed()` - Descending order
- `limit(10)` - Take top 10
- `collect()` - Materialize to list

---

### 5. **Failure Rates by Job Type**

**Location**: `AnalyticsService.getFailureRatesByType()`

```java
Map<String, Double> failureRates = getAllJobs().stream()
    .collect(Collectors.groupingBy(
        JobRepository.JobData::getType,
        Collectors.collectingAndThen(
            Collectors.toList(),
            jobs -> {
                long total = jobs.size();
                long failed = jobs.stream()
                    .filter(j -> j.getStatus() == JobStatus.FAILED)
                    .count();
                return total > 0 ? (failed * 100.0 / total) : 0.0;
            }
        )
    ));
```

**Operations**:
- `stream()` - Create stream
- `groupingBy()` - Group by job type
- `collectingAndThen()` - Transform collected data
- `filter()` - Count failed jobs
- `count()` - Count failures

**Output**:
```
EmailJob: 2.5%
CleanupJob: 5.0%
ReportJob: 1.2%
```

---

### 6. **Anti-Starvation Priority Calculation**

**Location**: `Scheduler.getPendingJobsWithAntiStarvation()`

```java
return allPendingJobs.stream()
    .sorted((job1, job2) -> {
        double effectivePriority1 = calculateEffectivePriority(job1, now);
        double effectivePriority2 = calculateEffectivePriority(job2, now);
        return Double.compare(effectivePriority2, effectivePriority1);
    })
    .limit(limit)
    .collect(Collectors.toList());
```

**Operations**:
- `stream()` - Create stream from pending jobs
- `sorted()` - Custom comparator with age bonus
- `limit()` - Take only what workers can handle
- `collect()` - Materialize to list

**Formula**: `effective_priority = base_priority + (age_minutes / 60)`

---

### 7. **Sorted Job Counts by Type (Display)**

**Location**: `Main.displayAnalytics()`

```java
analyticsService.getJobCountsByType()
    .entrySet().stream()
    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
    .forEach(entry -> 
        System.out.printf("  %-20s: %,d%n", entry.getKey(), entry.getValue())
    );
```

**Operations**:
- `entrySet().stream()` - Stream map entries
- `sorted()` - Sort by count (descending)
- `forEach()` - Print each entry

---

### Benefits of Streams API in This Project

1. **Declarative Code**: Expresses *what* to do, not *how*
2. **Lazy Evaluation**: Computations only happen when needed
3. **Pipeline Composition**: Chain multiple operations clearly
4. **Parallel Potential**: Easy to parallelize with `.parallelStream()`
5. **Type Safety**: Compile-time checks prevent errors
6. **Readability**: Analytics logic is concise and clear

---

## Future Enhancements

### 1. **Distributed Scheduling**
- Multiple scheduler instances for high availability
- Distributed locking (Redis, Zookeeper)
- Leader election for coordination
- Horizontal scaling

### 2. **Persistent Storage**
- Replace H2 with PostgreSQL or MySQL
- Separate read/write replicas for scalability
- Database sharding for large workloads

### 3. **Job Dependencies (DAG Execution)**
```java
Job job1 = new EmailJob();
Job job2 = new ReportJob();
job2.dependsOn(job1); // job2 waits for job1
```
- Directed Acyclic Graph (DAG) execution
- Parallel branch execution
- Conditional workflows

### 4. **Rate Limiting per Job Type**
```java
scheduler.setRateLimit("EmailJob", 100, TimeUnit.MINUTES);
```
- Prevent API rate limit violations
- Control resource consumption
- Token bucket or leaky bucket algorithm

### 5. **Advanced Metrics**
- **Percentiles**: P50, P95, P99 execution times
- **Histograms**: Execution time distribution
- **Counters**: Job submissions, failures, retries
- **Gauges**: Current queue depth, active workers
- Integration with Prometheus/Grafana

### 6. **Job Result Storage**
- Store job output in database
- Retrieve results via API: `GET /jobs/{id}/result`
- TTL for automatic cleanup

### 7. **Webhooks/Callbacks**
```java
job.onComplete(result -> 
    httpClient.post("https://callback.url", result)
);
```
- Notify external systems on job completion
- Retry webhook delivery on failure

### 8. **Scheduled/Recurring Jobs**
```java
scheduler.scheduleRecurring(job, "0 0 * * *"); // Daily at midnight
```
- Cron-like expressions for recurring tasks
- Timezone support

### 9. **Job Versioning**
- Multiple versions of same job type
- Rolling deployments without downtime
- Backward compatibility

### 10. **Observability**
- Distributed tracing (OpenTelemetry)
- Structured logging (JSON format)
- Correlation IDs across services
- APM integration (New Relic, Datadog)

### 11. **UI Dashboard**
- Web-based job queue management
- Real-time metrics visualization
- Job submission and cancellation
- DLQ management interface

### 12. **Priority Queues per Job Type**
- Separate queues for different job types
- Dedicated worker pools
- Better resource isolation

---

## License

This project is provided as-is for educational and demonstration purposes.

---

## Contributing

This is a demonstration project showcasing job queue design patterns and best practices. Feel free to use it as a reference for your own implementations!

---

## Contact

For questions or discussions about the architecture and design decisions, please open an issue on the repository.

---

**Built with ❤️ using Java 17, Maven, and H2 Database**

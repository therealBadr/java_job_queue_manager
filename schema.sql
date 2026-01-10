-- Job Queue Manager Database Schema

-- Drop tables if they exist (in reverse order of dependencies)
DROP TABLE IF EXISTS job_logs;
DROP TABLE IF EXISTS dead_letter_queue;
DROP TABLE IF EXISTS jobs;

-- Create jobs table
CREATE TABLE jobs (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255),
    type VARCHAR(100) NOT NULL,
    payload TEXT,
    priority INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    scheduled_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3
);

-- Create job_logs table
CREATE TABLE job_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    level VARCHAR(10) NOT NULL,
    message TEXT,
    CONSTRAINT fk_job_logs_job_id FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- Create dead_letter_queue table
CREATE TABLE dead_letter_queue (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255),
    type VARCHAR(100) NOT NULL,
    payload TEXT,
    priority INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    scheduled_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    original_error TEXT,
    moved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Create indexes for better query performance on jobs table
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_scheduled_time ON jobs(scheduled_time);
CREATE INDEX idx_jobs_priority ON jobs(priority);
CREATE INDEX idx_jobs_created_at ON jobs(created_at);
CREATE INDEX idx_jobs_type ON jobs(type);

-- Create indexes for job_logs table
CREATE INDEX idx_job_logs_job_id ON job_logs(job_id);
CREATE INDEX idx_job_logs_timestamp ON job_logs(timestamp);
CREATE INDEX idx_job_logs_level ON job_logs(level);

-- Create indexes for dead_letter_queue table
CREATE INDEX idx_dlq_status ON dead_letter_queue(status);
CREATE INDEX idx_dlq_moved_at ON dead_letter_queue(moved_at);
CREATE INDEX idx_dlq_type ON dead_letter_queue(type);

-- Sample data (optional - commented out)
-- INSERT INTO jobs (id, name, type, payload, priority, status, scheduled_time, created_at, retry_count, max_retries)
-- VALUES ('550e8400-e29b-41d4-a716-446655440000', 'Test Email Job', 'EmailJob', '{"to":"test@example.com","subject":"Test","body":"Hello"}', 5, 'PENDING', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, 3);

-- INSERT INTO job_logs (job_id, level, message)
-- VALUES ('550e8400-e29b-41d4-a716-446655440000', 'INFO', 'Job created');

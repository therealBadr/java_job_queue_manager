package com.jobqueue.core;

/**
 * Enum representing the various states a job can be in throughout its lifecycle.
 * 
 * <p>State Transitions:</p>
 * <ul>
 *   <li>PENDING → RUNNING: Job claimed by worker and started execution</li>
 *   <li>PENDING → CANCELLED: Job cancelled before execution started</li>
 *   <li>RUNNING → SUCCESS: Job completed successfully</li>
 *   <li>RUNNING → FAILED: Job execution failed (may trigger retry)</li>
 *   <li>RUNNING → CANCELLED: Job cancelled during execution</li>
 * </ul>
 * 
 * <p>Thread Safety: This enum is immutable and thread-safe.</p>
 * 
 * @author Job Queue Team
 * @see #canTransitionTo(JobStatus)
 */
public enum JobStatus {
    PENDING("Pending"),
    RUNNING("Running"),
    SUCCESS("Success"),
    FAILED("Failed"),
    CANCELLED("Cancelled");

    private final String displayName;

    JobStatus(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Get the human-readable display name for this status.
     * 
     * @return the display name (e.g., "Success", "Failed")
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Check if this status represents a terminal state.
     * Terminal states are final - jobs cannot transition out of them.
     * This is important for determining when a job is truly complete
     * and no further processing is needed.
     * 
     * @return true if the job has reached a final state (SUCCESS, FAILED, or CANCELLED)
     */
    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED;
    }

    /**
     * Validate if a transition to a new status is legal.
     * This enforces state machine rules to prevent invalid status updates.
     * 
     * <p>Key Invariant: Once a job reaches a terminal state (SUCCESS, FAILED, CANCELLED),
     * it cannot transition to any other state. This prevents accidental re-execution
     * of completed jobs.</p>
     * 
     * @param newStatus the target status to transition to
     * @return true if the transition is allowed, false if it violates state machine rules
     */
    public boolean canTransitionTo(JobStatus newStatus) {
        // Critical: Cannot transition from terminal states
        // This prevents completed jobs from being re-executed or modified
        if (this.isTerminal()) {
            return false;
        }

        // Define legal state transitions using switch expression
        return switch (this) {
            case PENDING -> newStatus == RUNNING || newStatus == CANCELLED;
            case RUNNING -> newStatus == SUCCESS || newStatus == FAILED || newStatus == CANCELLED;
            default -> false; // Should never reach here but prevents compilation warnings
        };
    }

    @Override
    public String toString() {
        return displayName;
    }
}

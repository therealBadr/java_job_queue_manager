package com.jobqueue.core;

/**
 * Enum representing the various states a job can be in
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

    public String getDisplayName() {
        return displayName;
    }

    /**
     * Check if this status represents a terminal state
     * @return true if the job has completed (SUCCESS, FAILED, or CANCELLED)
     */
    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED;
    }

    /**
     * Validate if transition to a new status is legal
     * @param newStatus the target status to transition to
     * @return true if the transition is allowed
     */
    public boolean canTransitionTo(JobStatus newStatus) {
        // Cannot transition from terminal states
        if (this.isTerminal()) {
            return false;
        }

        // Define legal transitions
        return switch (this) {
            case PENDING -> newStatus == RUNNING || newStatus == CANCELLED;
            case RUNNING -> newStatus == SUCCESS || newStatus == FAILED || newStatus == CANCELLED;
            default -> false;
        };
    }

    @Override
    public String toString() {
        return displayName;
    }
}

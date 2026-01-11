package com.jobqueue.core;

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

    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == CANCELLED;
    }

    public boolean canTransitionTo(JobStatus newStatus) {
        if (this.isTerminal()) {
            return false;
        }

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

package core;

/**
 * Represents the lifecycle status of a job in the queue system.
 * 
 * State Transition Flow:
 * PENDING -> RUNNING -> SUCCESS (happy path)
 *                    -> FAILED (error path)
 * 
 * Important: Once a job reaches SUCCESS or FAILED, it should NOT
 * transition back to other states. If retry logic is needed (bonus),
 * create a NEW job instead of modifying the existing one.
 */
public enum JobStatus {

    PENDING,

    RUNNING,

    SUCCESS,

    FAILED;

    public boolean isCompleted() {
        return this == SUCCESS || this == FAILED;
    }

    public boolean isActive() {
        return this == PENDING || this == RUNNING;
    }
}


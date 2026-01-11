package com.jobqueue.core;

public interface Job {
    String getId();
    
    String getType();
    
    void execute(JobContext context) throws Exception;
    
    String getPayload();
    
    int getPriority();
    
    int getMaxRetries();

    boolean isCancellable();
}

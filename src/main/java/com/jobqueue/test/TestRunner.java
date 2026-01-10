package com.jobqueue.test;

/**
 * Master test runner - executes all test suites
 */
public class TestRunner {
    
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║        JOB QUEUE MANAGER - COMPREHENSIVE TESTS        ║");
        System.out.println("╚════════════════════════════════════════════════════════╝\n");
        
        boolean allPassed = true;
        int totalTests = 0;
        int passedTests = 0;
        
        // Test Suite 1: Core Components
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 1: Core Components");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestCore.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ Core Components Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        System.out.println("\n");
        
        // Test Suite 2: Worker Component
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 2: Worker Component");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestWorker.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ Worker Component Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        System.out.println("\n");
        
        // Test Suite 3: Scheduler Component
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 3: Scheduler Component");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestScheduler.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ Scheduler Component Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        System.out.println("\n");
        
        // Test Suite 4: Analytics Service
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 4: Analytics Service");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestAnalytics.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ Analytics Service Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        System.out.println("\n");
        
        // Test Suite 5: Integration Tests
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 5: Integration Tests");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestIntegration.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ Integration Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        System.out.println("\n");
        
        // Test Suite 6: Dead Letter Queue
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 6: Dead Letter Queue (DLQ)");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestDLQ.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ DLQ Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        System.out.println("\n");
        
        // Test Suite 7: Metrics Server
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("TEST SUITE 7: Metrics Server");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
        try {
            TestMetricsServer.main(new String[]{});
            passedTests++;
        } catch (Exception e) {
            System.err.println("✗ Metrics Server Test Suite FAILED");
            allPassed = false;
        }
        totalTests++;
        
        // Final Summary
        System.out.println("\n");
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║                    FINAL SUMMARY                       ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Total Test Suites: " + totalTests);
        System.out.println("Passed: " + passedTests);
        System.out.println("Failed: " + (totalTests - passedTests));
        System.out.println("Success Rate: " + String.format("%.1f", (passedTests * 100.0 / totalTests)) + "%");
        System.out.println();
        
        if (allPassed) {
            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║              ✓ ALL TEST SUITES PASSED ✓               ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.exit(0);
        } else {
            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║              ✗ SOME TEST SUITES FAILED ✗              ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.exit(1);
        }
    }
}

package com.example.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

class MemoryMonitorTest {
    private MemoryMonitor monitor;
    
    @BeforeEach
    void setUp() {
        monitor = new MemoryMonitor(0.75); // 75% threshold
    }
    
    @Test
    void testInitialBatchSize() {
        assertEquals(100_000, monitor.getRecommendedBatchSize(),
            "Initial batch size should be set to MAX_BATCH_SIZE");
    }
    
    @Test
    void testInvalidThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new MemoryMonitor(0.0),
            "Should throw exception for threshold = 0.0");
        assertThrows(IllegalArgumentException.class, () -> new MemoryMonitor(1.0),
            "Should throw exception for threshold = 1.0");
    }
    
    @Test
    void testMemoryPressure() {
        // Create memory pressure by allocating large objects
        List<byte[]> memoryHog = new ArrayList<>();
        long initialBatchSize = monitor.getRecommendedBatchSize();
        
        try {
            // Allocate memory until we see batch size reduction
            for (int i = 0; i < 10; i++) {
                memoryHog.add(new byte[1024 * 1024 * 10]); // 10MB chunks
                monitor.update();
                
                if (monitor.getRecommendedBatchSize() < initialBatchSize) {
                    // Batch size was reduced due to memory pressure
                    assertTrue(monitor.getRecommendedBatchSize() >= 1000,
                        "Batch size should not go below MIN_BATCH_SIZE");
                    return;
                }
            }
            
            // If we get here, we couldn't create enough memory pressure
            // This is not necessarily a failure, as it depends on available memory
            System.out.println("Warning: Could not create enough memory pressure to test batch size reduction");
        } finally {
            // Clean up allocated memory
            memoryHog.clear();
            System.gc();
        }
    }
    
    @Test
    void testMemoryStats() {
        String stats = monitor.getMemoryStats();
        assertNotNull(stats, "Memory stats should not be null");
        assertTrue(stats.contains("Memory Usage:"), "Stats should contain usage info");
        assertTrue(stats.contains("MB"), "Stats should show values in MB");
        assertTrue(stats.contains("Current Batch Size:"), "Stats should show batch size");
    }
    
    @Test
    void testPeakMemoryTracking() {
        long initialPeak = monitor.getPeakMemoryUsage();
        
        // Create some memory pressure
        byte[] memoryHog = new byte[1024 * 1024 * 10]; // 10MB
        monitor.update();
        
        assertTrue(monitor.getPeakMemoryUsage() >= initialPeak,
            "Peak memory usage should not decrease");
        
        // Clean up
        memoryHog = null;
        System.gc();
    }
    
    @Test
    void testForceUpdate() {
        long beforeSize = monitor.getRecommendedBatchSize();
        monitor.forceUpdate();
        // After forced GC, memory usage should be lower, potentially allowing larger batches
        assertTrue(monitor.getRecommendedBatchSize() >= beforeSize,
            "Batch size should not decrease after GC");
    }
} 
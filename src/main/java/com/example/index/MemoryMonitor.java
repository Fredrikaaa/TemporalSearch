package com.example.index;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Monitors memory usage and provides adaptive batch sizing recommendations.
 * Uses JMX to track heap memory usage and adjusts batch sizes based on memory pressure.
 */
public class MemoryMonitor {
    private static final Logger logger = Logger.getLogger(MemoryMonitor.class.getName());
    private static final double DEFAULT_MEMORY_THRESHOLD = 0.75; // 75% memory usage threshold
    private static final long MIN_BATCH_SIZE = 1000;
    private static final long MAX_BATCH_SIZE = 100_000;
    
    private final MemoryMXBean memoryBean;
    private final double memoryThreshold;
    private final AtomicLong currentBatchSize;
    private final AtomicLong peakMemoryUsage;
    
    /**
     * Creates a new MemoryMonitor with default settings.
     */
    public MemoryMonitor() {
        this(DEFAULT_MEMORY_THRESHOLD);
    }
    
    /**
     * Creates a new MemoryMonitor with a custom memory threshold.
     * 
     * @param memoryThreshold Memory usage threshold (0.0 to 1.0) that triggers batch size adjustments
     */
    public MemoryMonitor(double memoryThreshold) {
        if (memoryThreshold <= 0.0 || memoryThreshold >= 1.0) {
            throw new IllegalArgumentException("Memory threshold must be between 0.0 and 1.0");
        }
        
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.memoryThreshold = memoryThreshold;
        this.currentBatchSize = new AtomicLong(MAX_BATCH_SIZE);
        this.peakMemoryUsage = new AtomicLong(0);
    }
    
    /**
     * Gets the recommended batch size based on current memory usage.
     * 
     * @return The recommended batch size
     */
    public long getRecommendedBatchSize() {
        return currentBatchSize.get();
    }
    
    /**
     * Gets the peak memory usage observed since monitoring began.
     * 
     * @return Peak memory usage in bytes
     */
    public long getPeakMemoryUsage() {
        return peakMemoryUsage.get();
    }
    
    /**
     * Updates memory statistics and adjusts the recommended batch size.
     * Should be called periodically during processing.
     */
    public void update() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        long used = heapUsage.getUsed();
        long max = heapUsage.getMax();
        
        // Update peak memory usage
        peakMemoryUsage.updateAndGet(peak -> Math.max(peak, used));
        
        // Calculate memory usage ratio
        double usageRatio = (double) used / max;
        
        if (usageRatio > memoryThreshold) {
            // Memory usage is high, reduce batch size
            long newSize = Math.max(MIN_BATCH_SIZE, 
                                  currentBatchSize.get() / 2);
            currentBatchSize.set(newSize);
            
            logger.info(String.format(
                "High memory usage (%.1f%%), reducing batch size to %d",
                usageRatio * 100, newSize));
            
            // Suggest garbage collection if memory usage is very high
            if (usageRatio > 0.9) {
                logger.warning("Memory usage critical, suggesting garbage collection");
                System.gc();
            }
        } else if (usageRatio < memoryThreshold / 2) {
            // Memory usage is low, try to increase batch size
            long newSize = Math.min(MAX_BATCH_SIZE, 
                                  currentBatchSize.get() * 2);
            currentBatchSize.set(newSize);
            
            logger.info(String.format(
                "Low memory usage (%.1f%%), increasing batch size to %d",
                usageRatio * 100, newSize));
        }
    }
    
    /**
     * Forces a garbage collection and memory usage update.
     * Use sparingly, as forcing GC can be expensive.
     */
    public void forceUpdate() {
        System.gc();
        update();
    }
    
    /**
     * Gets the current memory usage statistics.
     * 
     * @return A string containing current memory usage information
     */
    public String getMemoryStats() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return String.format(
            "Memory Usage: %d MB used of %d MB max (%.1f%%), Peak: %d MB, Current Batch Size: %d",
            heapUsage.getUsed() / (1024 * 1024),
            heapUsage.getMax() / (1024 * 1024),
            (double) heapUsage.getUsed() / heapUsage.getMax() * 100,
            peakMemoryUsage.get() / (1024 * 1024),
            currentBatchSize.get()
        );
    }
} 
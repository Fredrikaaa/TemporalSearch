package com.example.logging;

import org.slf4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced batch statistics tracking with detailed performance metrics.
 * Thread-safe for concurrent access during parallel processing.
 */
public class BatchStats {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    
    private final AtomicInteger totalDocuments;
    private final AtomicInteger documentsWithNulls;
    private final AtomicInteger documentsWithErrors;
    private final AtomicLong processingTimeNanos;
    private final AtomicLong maxProcessingTimeNanos;
    private final AtomicLong minProcessingTimeNanos;
    private final long startTime;
    private final long initialHeapUsed;
    
    public BatchStats() {
        this.totalDocuments = new AtomicInteger(0);
        this.documentsWithNulls = new AtomicInteger(0);
        this.documentsWithErrors = new AtomicInteger(0);
        this.processingTimeNanos = new AtomicLong(0);
        this.maxProcessingTimeNanos = new AtomicLong(0);
        this.minProcessingTimeNanos = new AtomicLong(Long.MAX_VALUE);
        this.startTime = System.nanoTime();
        this.initialHeapUsed = memoryBean.getHeapMemoryUsage().getUsed();
    }
    
    public void recordDocumentProcessing(long processingTimeNanos) {
        this.totalDocuments.incrementAndGet();
        this.processingTimeNanos.addAndGet(processingTimeNanos);
        updateProcessingTimeStats(processingTimeNanos);
    }
    
    private void updateProcessingTimeStats(long timeNanos) {
        while (true) {
            long currentMax = maxProcessingTimeNanos.get();
            if (timeNanos <= currentMax || 
                maxProcessingTimeNanos.compareAndSet(currentMax, timeNanos)) {
                break;
            }
        }
        
        while (true) {
            long currentMin = minProcessingTimeNanos.get();
            if (timeNanos >= currentMin || 
                minProcessingTimeNanos.compareAndSet(currentMin, timeNanos)) {
                break;
            }
        }
    }
    
    public void incrementNulls() {
        documentsWithNulls.incrementAndGet();
    }
    
    public void incrementErrors() {
        documentsWithErrors.incrementAndGet();
    }
    
    /**
     * Logs enhanced batch processing statistics in JSON format.
     * Includes processing times, memory usage, and error counts.
     */
    public void logBatchSummary(Logger logger) {
        try {
            long totalTimeNanos = System.nanoTime() - startTime;
            long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
            long heapMax = memoryBean.getHeapMemoryUsage().getMax();
            
            ObjectNode json = MAPPER.createObjectNode()
                .put("event", "batch_complete")
                .put("total_documents", totalDocuments.get())
                .put("documents_with_nulls", documentsWithNulls.get())
                .put("documents_with_errors", documentsWithErrors.get())
                .put("total_time_ms", totalTimeNanos / 1_000_000.0)
                .put("avg_processing_time_ms", getAverageProcessingTimeMs())
                .put("max_processing_time_ms", maxProcessingTimeNanos.get() / 1_000_000.0)
                .put("min_processing_time_ms", 
                    minProcessingTimeNanos.get() == Long.MAX_VALUE ? 0 : 
                    minProcessingTimeNanos.get() / 1_000_000.0)
                .put("throughput_docs_per_sec", calculateThroughput(totalTimeNanos))
                .put("heap_used_mb", heapUsed / (1024 * 1024))
                .put("heap_max_mb", heapMax / (1024 * 1024))
                .put("heap_usage_percent", (heapUsed * 100.0) / heapMax)
                .put("heap_increase_mb", (heapUsed - initialHeapUsed) / (1024 * 1024));
            
            logger.info(json.toString());
        } catch (Exception e) {
            // Fallback to simple logging if JSON creation fails
            logger.info("Batch complete - total: {}, nulls: {}, errors: {}, avg_time_ms: {}",
                totalDocuments.get(), documentsWithNulls.get(), documentsWithErrors.get(),
                getAverageProcessingTimeMs());
        }
    }
    
    private double getAverageProcessingTimeMs() {
        int total = totalDocuments.get();
        if (total == 0) return 0.0;
        return (processingTimeNanos.get() / (double) total) / 1_000_000.0;
    }
    
    private double calculateThroughput(long totalTimeNanos) {
        double seconds = totalTimeNanos / 1_000_000_000.0;
        if (seconds == 0) return 0.0;
        return totalDocuments.get() / seconds;
    }
    
    // Getters for testing and verification
    public int getTotalDocuments() {
        return totalDocuments.get();
    }
    
    public int getDocumentsWithNulls() {
        return documentsWithNulls.get();
    }
    
    public int getDocumentsWithErrors() {
        return documentsWithErrors.get();
    }
    
    public long getTotalProcessingTimeNanos() {
        return processingTimeNanos.get();
    }
} 
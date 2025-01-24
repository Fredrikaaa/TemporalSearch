package com.example.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks statistics for a batch of documents being processed.
 * Uses sampling to reduce log volume while maintaining visibility.
 */
public class BatchStats {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MemoryMXBean MEMORY_BEAN = ManagementFactory.getMemoryMXBean();
    private static final LogSampler BATCH_SAMPLER = new LogSampler(0.05); // Sample 5% of batches

    private final List<Long> processingTimes;
    private final AtomicInteger nullCount;
    private final AtomicInteger errorCount;
    private final long batchStartTime;
    private final AtomicLong processingTimeNanos;
    private final AtomicLong maxProcessingTimeNanos;
    private final AtomicLong minProcessingTimeNanos;
    private final long startTime;
    private final long initialHeapUsed;

    public BatchStats() {
        this.processingTimes = new ArrayList<>();
        this.nullCount = new AtomicInteger(0);
        this.errorCount = new AtomicInteger(0);
        this.processingTimeNanos = new AtomicLong(0);
        this.maxProcessingTimeNanos = new AtomicLong(0);
        this.minProcessingTimeNanos = new AtomicLong(Long.MAX_VALUE);
        this.batchStartTime = System.currentTimeMillis();
        this.startTime = System.nanoTime();
        this.initialHeapUsed = MEMORY_BEAN.getHeapMemoryUsage().getUsed();
    }

    public synchronized void recordDocumentProcessing(long timeNanos) {
        processingTimes.add(timeNanos / 1_000_000); // Convert to milliseconds
        processingTimeNanos.addAndGet(timeNanos);
        updateProcessingTimeStats(timeNanos);
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
        nullCount.incrementAndGet();
    }

    public void incrementErrors() {
        errorCount.incrementAndGet();
    }

    /**
     * Logs batch completion metrics with sampling to reduce volume.
     * Critical errors and significant deviations are always logged.
     */
    public void logBatchSummary(Logger logger) {
        int totalDocs = processingTimes.size();
        int nullDocs = nullCount.get();
        int errorDocs = errorCount.get();
        
        // Always log if there are errors or high null rate
        boolean hasErrors = errorDocs > 0;
        boolean highNullRate = nullDocs > totalDocs * 0.1; // > 10% nulls
        
        if (hasErrors || highNullRate || BATCH_SAMPLER.shouldLog()) {
            try {
                double avgProcessingTime = processingTimes.stream()
                    .mapToLong(Long::valueOf)
                    .average()
                    .orElse(0.0);

                long elapsedMs = System.currentTimeMillis() - batchStartTime;
                double throughput = elapsedMs > 0 ? (totalDocs * 1000.0 / elapsedMs) : 0.0;

                ObjectNode json = MAPPER.createObjectNode()
                    .put("event", "batch_complete")
                    .put("total_documents", totalDocs)
                    .put("documents_with_nulls", nullDocs)
                    .put("documents_with_errors", errorDocs)
                    .put("avg_processing_time_ms", avgProcessingTime)
                    .put("throughput_docs_per_sec", throughput)
                    .put("heap_used_mb", MEMORY_BEAN.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0));

                // Use appropriate log level based on conditions
                if (hasErrors) {
                    logger.warn(json.toString());
                } else if (highNullRate) {
                    logger.warn(json.toString());
                } else {
                    logger.info(json.toString());
                }
            } catch (Exception e) {
                logger.warn("Failed to log batch summary", e);
            }
        }
    }

    private double getAverageProcessingTimeMs() {
        int total = processingTimes.size();
        if (total == 0) return 0.0;
        return (processingTimeNanos.get() / (double) total) / 1_000_000.0;
    }

    private double calculateThroughput(long totalTimeNanos) {
        double seconds = totalTimeNanos / 1_000_000_000.0;
        if (seconds == 0) return 0.0;
        return processingTimes.size() / seconds;
    }

    // Getters for testing and verification
    public int getTotalDocuments() {
        return processingTimes.size();
    }
    
    public int getDocumentsWithNulls() {
        return nullCount.get();
    }
    
    public int getDocumentsWithErrors() {
        return errorCount.get();
    }
    
    public long getTotalProcessingTimeNanos() {
        return processingTimeNanos.get();
    }
} 
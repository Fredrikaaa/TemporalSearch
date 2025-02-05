package com.example.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Unified metrics tracking for the indexing process.
 * Handles both high-level indexing metrics and batch-level processing details.
 * Uses sampling to reduce log volume while maintaining visibility.
 */
public class IndexingMetrics {
    private static final Logger logger = LoggerFactory.getLogger(IndexingMetrics.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MemoryMXBean MEMORY_BEAN = ManagementFactory.getMemoryMXBean();
    
    // Sampling configuration
    private final LogSampler detailedMetricsSampler;
    private final LogSampler batchMetricsSampler;
    
    // Overall metrics
    private final long startTime;
    private final long initialHeapUsed;
    private final AtomicLong totalProcessingTimeNanos;
    private final AtomicInteger totalDocumentsProcessed;
    private final AtomicInteger totalBatchesProcessed;
    
    // Batch-level metrics
    private final AtomicInteger nullCount;
    private final AtomicInteger errorCount;
    private final AtomicLong maxProcessingTimeNanos;
    private final AtomicLong minProcessingTimeNanos;
    private final List<Long> recentProcessingTimes;
    
    // Current batch tracking
    private long currentBatchStartTime;
    private int currentBatchSize;

    public IndexingMetrics() {
        this.startTime = System.nanoTime();
        this.initialHeapUsed = MEMORY_BEAN.getHeapMemoryUsage().getUsed();
        
        // Initialize samplers
        this.detailedMetricsSampler = new LogSampler(0.1);  // 10% sampling for detailed metrics
        this.batchMetricsSampler = new LogSampler(0.05);    // 5% sampling for batch metrics
        
        // Initialize counters
        this.totalProcessingTimeNanos = new AtomicLong(0);
        this.totalDocumentsProcessed = new AtomicInteger(0);
        this.totalBatchesProcessed = new AtomicInteger(0);
        this.nullCount = new AtomicInteger(0);
        this.errorCount = new AtomicInteger(0);
        this.maxProcessingTimeNanos = new AtomicLong(0);
        this.minProcessingTimeNanos = new AtomicLong(Long.MAX_VALUE);
        this.recentProcessingTimes = new ArrayList<>();
    }

    /**
     * Start tracking a new batch of documents.
     * @param batchSize The number of documents in this batch
     */
    public void startBatch(int batchSize) {
        this.currentBatchStartTime = System.nanoTime();
        this.currentBatchSize = batchSize;
    }

    /**
     * Record successful processing of the current batch.
     */
    public void recordBatchSuccess() {
        long duration = System.nanoTime() - currentBatchStartTime;
        recordBatchCompletion(duration, true);
    }

    /**
     * Record a failed batch processing attempt.
     */
    public void recordBatchFailure() {
        long duration = System.nanoTime() - currentBatchStartTime;
        recordBatchCompletion(duration, false);
        errorCount.incrementAndGet();
    }

    /**
     * Record a null or empty batch result.
     */
    public void recordNullBatch() {
        nullCount.incrementAndGet();
    }

    private synchronized void recordBatchCompletion(long durationNanos, boolean success) {
        // Update timing statistics
        totalProcessingTimeNanos.addAndGet(durationNanos);
        updateProcessingTimeStats(durationNanos);
        
        // Update document counts
        if (success) {
            totalDocumentsProcessed.addAndGet(currentBatchSize);
        }
        totalBatchesProcessed.incrementAndGet();
        
        // Keep recent processing times for averaging
        recentProcessingTimes.add(durationNanos);
        if (recentProcessingTimes.size() > 100) { // Keep last 100 batches
            recentProcessingTimes.remove(0);
        }
        
        // Log batch metrics if sampled
        if (batchMetricsSampler.shouldLog() || !success) {
            logBatchMetrics(durationNanos, success);
        }
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

    private void logBatchMetrics(long durationNanos, boolean success) {
        try {
            double avgProcessingTime = recentProcessingTimes.stream()
                .mapToLong(Long::valueOf)
                .average()
                .orElse(0.0);

            ObjectNode json = MAPPER.createObjectNode()
                .put("event", "batch_complete")
                .put("success", success)
                .put("batch_size", currentBatchSize)
                .put("processing_time_ms", durationNanos / 1_000_000.0)
                .put("avg_processing_time_ms", avgProcessingTime / 1_000_000.0)
                .put("total_documents", totalDocumentsProcessed.get())
                .put("total_batches", totalBatchesProcessed.get())
                .put("errors", errorCount.get())
                .put("nulls", nullCount.get())
                .put("heap_used_mb", MEMORY_BEAN.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0));

            if (success) {
                logger.info(json.toString());
            } else {
                logger.warn(json.toString());
            }
        } catch (Exception e) {
            logger.warn("Failed to log batch metrics", e);
        }
    }

    /**
     * Log overall indexing metrics. Called periodically or at completion.
     */
    public void logIndexingMetrics() {
        try {
            long elapsedNanos = System.nanoTime() - startTime;
            double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
            
            ObjectNode json = MAPPER.createObjectNode()
                .put("event", "indexing_metrics")
                .put("total_documents", totalDocumentsProcessed.get())
                .put("total_batches", totalBatchesProcessed.get())
                .put("total_errors", errorCount.get())
                .put("total_nulls", nullCount.get())
                .put("elapsed_seconds", elapsedSeconds)
                .put("docs_per_second", totalDocumentsProcessed.get() / elapsedSeconds)
                .put("avg_batch_size", (double) totalDocumentsProcessed.get() / totalBatchesProcessed.get())
                .put("min_batch_time_ms", minProcessingTimeNanos.get() / 1_000_000.0)
                .put("max_batch_time_ms", maxProcessingTimeNanos.get() / 1_000_000.0)
                .put("heap_used_mb", MEMORY_BEAN.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0))
                .put("heap_change_mb", (MEMORY_BEAN.getHeapMemoryUsage().getUsed() - initialHeapUsed) / (1024.0 * 1024.0));

            logger.info(json.toString());
        } catch (Exception e) {
            logger.warn("Failed to log indexing metrics", e);
        }
    }

    // Getters for testing and verification
    public int getTotalDocuments() {
        return totalDocumentsProcessed.get();
    }
    
    public int getTotalBatches() {
        return totalBatchesProcessed.get();
    }
    
    public int getErrorCount() {
        return errorCount.get();
    }
    
    public int getNullCount() {
        return nullCount.get();
    }
    
    public long getTotalProcessingTimeNanos() {
        return totalProcessingTimeNanos.get();
    }
} 
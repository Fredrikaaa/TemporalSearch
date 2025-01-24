package com.example.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Tracks detailed performance metrics and state verification during indexing.
 * Thread-safe for concurrent access during parallel processing.
 */
public class IndexingMetrics {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(IndexingMetrics.class);
    private final MemoryMXBean memoryBean;
    private final ThreadMXBean threadBean;
    private final AtomicLong totalProcessingTime;
    private final AtomicLong documentsProcessed;
    private final AtomicLong ngramsGenerated;
    private final Map<String, AtomicLong> stateVerificationCounts;
    private final long startTime;
    private final LogSampler detailedMetricsSampler;
    private final LogSampler stateVerificationSampler;

    public IndexingMetrics() {
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.threadBean = ManagementFactory.getThreadMXBean();
        this.totalProcessingTime = new AtomicLong(0);
        this.documentsProcessed = new AtomicLong(0);
        this.ngramsGenerated = new AtomicLong(0);
        this.stateVerificationCounts = new ConcurrentHashMap<>();
        this.startTime = System.currentTimeMillis();
        // Sample 100% of detailed metrics and 10% of state verifications
        this.detailedMetricsSampler = new LogSampler(1.0);
        this.stateVerificationSampler = new LogSampler(0.1);
    }

    public void recordProcessingTime(long timeMs) {
        totalProcessingTime.addAndGet(timeMs);
    }

    public void incrementDocumentsProcessed() {
        documentsProcessed.incrementAndGet();
    }

    public void incrementDocumentsProcessed(int count) {
        documentsProcessed.addAndGet(count);
    }

    public void addNgramsGenerated(long count) {
        ngramsGenerated.addAndGet(count);
    }

    public void recordStateVerification(String state, boolean passed) {
        // Only log a sample of state verifications to reduce volume
        if (stateVerificationSampler.shouldLog()) {
            stateVerificationCounts.computeIfAbsent(state + "_" + (passed ? "passed" : "failed"),
                k -> new AtomicLong()).incrementAndGet();
        }
    }

    /**
     * Logs current metrics in JSON format.
     * Uses sampling for detailed metrics to reduce log volume.
     */
    public void logMetrics(String phase) {
        // Always log summary metrics with processing time
        ObjectNode summaryJson = MAPPER.createObjectNode()
            .put("event", "indexing_summary")
            .put("phase", phase)
            .put("documents_processed", documentsProcessed.get())
            .put("ngrams_generated", ngramsGenerated.get())
            .put("total_processing_ms", totalProcessingTime.get())
            .put("throughput_docs_per_sec", calculateThroughput());
        
        logger.info(summaryJson.toString());

        // Sample detailed metrics
        if (detailedMetricsSampler.shouldLog()) {
            try {
                ObjectNode detailedJson = MAPPER.createObjectNode()
                    .put("event", "indexing_metrics")
                    .put("phase", phase)
                    .put("elapsed_ms", System.currentTimeMillis() - startTime)
                    .put("total_processing_ms", totalProcessingTime.get())
                    .put("heap_used_mb", memoryBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0))
                    .put("heap_max_mb", memoryBean.getHeapMemoryUsage().getMax() / (1024.0 * 1024.0))
                    .put("thread_count", threadBean.getThreadCount())
                    .put("peak_thread_count", threadBean.getPeakThreadCount())
                    .put("avg_processing_time_per_doc_ms", documentsProcessed.get() > 0 ? 
                        totalProcessingTime.get() / (double)documentsProcessed.get() : 0.0);

                logger.debug(detailedJson.toString());
            } catch (Exception e) {
                logger.warn("Failed to log detailed metrics", e);
            }
        }
    }

    private double calculateThroughput() {
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        if (elapsedSeconds == 0) return 0.0;
        return documentsProcessed.get() / (double) elapsedSeconds;
    }

    public long getDocumentsProcessed() {
        return documentsProcessed.get();
    }

    public long getNgramsGenerated() {
        return ngramsGenerated.get();
    }

    public Map<String, Long> getStateVerificationCounts() {
        Map<String, Long> counts = new ConcurrentHashMap<>();
        stateVerificationCounts.forEach((state, count) -> counts.put(state, count.get()));
        return counts;
    }
} 
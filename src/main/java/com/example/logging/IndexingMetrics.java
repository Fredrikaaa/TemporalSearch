package com.example.logging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;

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
    private final MemoryMXBean memoryBean;
    private final ThreadMXBean threadBean;
    private final AtomicLong totalProcessingTime;
    private final AtomicLong documentsProcessed;
    private final AtomicLong ngramsGenerated;
    private final Map<String, AtomicLong> stateVerificationCounts;
    private final long startTime;

    public IndexingMetrics() {
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.threadBean = ManagementFactory.getThreadMXBean();
        this.totalProcessingTime = new AtomicLong(0);
        this.documentsProcessed = new AtomicLong(0);
        this.ngramsGenerated = new AtomicLong(0);
        this.stateVerificationCounts = new ConcurrentHashMap<>();
        this.startTime = System.currentTimeMillis();
    }

    public void recordProcessingTime(long timeMs) {
        totalProcessingTime.addAndGet(timeMs);
    }

    public void incrementDocumentsProcessed() {
        documentsProcessed.incrementAndGet();
    }

    public void addNgramsGenerated(long count) {
        ngramsGenerated.addAndGet(count);
    }

    public void recordStateVerification(String state, boolean passed) {
        stateVerificationCounts.computeIfAbsent(state + "_" + (passed ? "passed" : "failed"),
            k -> new AtomicLong()).incrementAndGet();
    }

    /**
     * Logs current metrics in JSON format.
     * Includes memory usage, thread stats, and processing metrics.
     */
    public void logMetrics(Logger logger, String phase) {
        try {
            ObjectNode json = MAPPER.createObjectNode()
                .put("event", "indexing_metrics")
                .put("phase", phase)
                .put("elapsed_ms", System.currentTimeMillis() - startTime)
                .put("total_processing_ms", totalProcessingTime.get())
                .put("documents_processed", documentsProcessed.get())
                .put("ngrams_generated", ngramsGenerated.get())
                .put("throughput_docs_per_sec", calculateThroughput())
                .put("heap_used_mb", memoryBean.getHeapMemoryUsage().getUsed() / (1024 * 1024))
                .put("heap_max_mb", memoryBean.getHeapMemoryUsage().getMax() / (1024 * 1024))
                .put("active_threads", threadBean.getThreadCount());

            // Add state verification counts
            ObjectNode verificationNode = json.putObject("state_verifications");
            stateVerificationCounts.forEach((state, count) -> 
                verificationNode.put(state, count.get()));

            logger.info(json.toString());
        } catch (Exception e) {
            logger.warn("Failed to log metrics: {}", e.getMessage());
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
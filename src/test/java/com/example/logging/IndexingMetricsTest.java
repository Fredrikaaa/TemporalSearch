package com.example.logging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class IndexingMetricsTest {
    private IndexingMetrics metrics;
    private ListAppender<ILoggingEvent> listAppender;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        metrics = new IndexingMetrics();
        
        // Setup logger capture
        Logger logger = (Logger) LoggerFactory.getLogger(IndexingMetrics.class);
        logger.setLevel(ch.qos.logback.classic.Level.INFO);  // Set level to INFO
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @Test
    void testSuccessfulBatchProcessing() throws Exception {
        // Process a successful batch
        metrics.startBatch(100);
        Thread.sleep(10); // Simulate some work
        metrics.recordBatchSuccess();

        assertEquals(100, metrics.getTotalDocuments());
        assertEquals(1, metrics.getTotalBatches());
        assertEquals(0, metrics.getErrorCount());
        assertEquals(0, metrics.getNullCount());
        assertTrue(metrics.getTotalProcessingTimeNanos() > 0);
    }

    @Test
    void testFailedBatchProcessing() {
        metrics.startBatch(100);
        metrics.recordBatchFailure();

        assertEquals(0, metrics.getTotalDocuments());
        assertEquals(1, metrics.getTotalBatches());
        assertEquals(1, metrics.getErrorCount());
    }

    @Test
    void testNullBatchProcessing() {
        metrics.startBatch(100);
        metrics.recordNullBatch();

        assertEquals(0, metrics.getTotalDocuments());
        assertEquals(0, metrics.getTotalBatches());
        assertEquals(0, metrics.getErrorCount());
        assertEquals(1, metrics.getNullCount());
    }

    @Test
    void testMetricsLogging() throws Exception {
        // Process multiple batches to test logging
        for (int i = 0; i < 5; i++) {
            metrics.startBatch(100);
            if (i % 2 == 0) {
                metrics.recordBatchSuccess();
            } else {
                metrics.recordBatchFailure();
            }
        }

        // Force metrics logging
        metrics.logIndexingMetrics();

        // Verify log output
        assertFalse(listAppender.list.isEmpty());
        
        // Find the indexing_metrics event
        JsonNode indexingMetrics = null;
        for (ILoggingEvent event : listAppender.list) {
            JsonNode json = MAPPER.readTree(event.getMessage());
            if (json.get("event").asText().equals("indexing_metrics")) {
                indexingMetrics = json;
                break;
            }
        }
        
        assertNotNull(indexingMetrics, "No indexing_metrics event found");
        assertEquals(300, indexingMetrics.get("total_documents").asInt()); // 3 successful batches * 100
        assertEquals(5, indexingMetrics.get("total_batches").asInt());
        assertEquals(2, indexingMetrics.get("total_errors").asInt());
        assertTrue(indexingMetrics.has("elapsed_seconds"));
        assertTrue(indexingMetrics.has("docs_per_second"));
        assertTrue(indexingMetrics.has("heap_used_mb"));
    }

    @Test
    void testPerformanceOverhead() {
        long startTime = System.nanoTime();
        
        // Process 1000 batches quickly
        for (int i = 0; i < 1000; i++) {
            metrics.startBatch(100);
            metrics.recordBatchSuccess();
        }
        
        long duration = System.nanoTime() - startTime;
        
        // Ensure overhead is reasonable (less than 1ms per batch on average)
        assertTrue(duration / 1_000_000.0 < 1000.0,
            "Metrics overhead too high: " + (duration / 1_000_000.0) + "ms for 1000 batches");
    }
} 
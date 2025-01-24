package com.example.index;

import com.example.logging.BatchStats;
import com.example.logging.IndexingMetrics;
import org.junit.jupiter.api.Test;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;

class LoggingTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testBatchStatsLogging() throws Exception {
        // Setup test appender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        ch.qos.logback.classic.Logger logger = 
            (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(BatchStats.class);
        logger.addAppender(listAppender);
        logger.setLevel(Level.INFO);

        // Create and use BatchStats
        BatchStats stats = new BatchStats();
        stats.recordDocumentProcessing(1_000_000); // 1ms
        stats.recordDocumentProcessing(2_000_000); // 2ms
        stats.incrementNulls();
        stats.incrementErrors();
        
        // Log summary - should always log due to errors
        stats.logBatchSummary(logger);

        // Verify log output - should have a WARN level message due to errors
        assertFalse(listAppender.list.isEmpty());
        ILoggingEvent event = listAppender.list.get(0);
        assertEquals("WARN", event.getLevel().toString());
        
        String logMessage = event.getMessage();
        JsonNode json = MAPPER.readTree(logMessage);

        assertEquals("batch_complete", json.get("event").asText());
        assertEquals(2, json.get("total_documents").asInt());
        assertEquals(1, json.get("documents_with_nulls").asInt());
        assertEquals(1, json.get("documents_with_errors").asInt());
        assertTrue(json.has("avg_processing_time_ms"));
        assertTrue(json.has("heap_used_mb"));
        assertTrue(json.has("throughput_docs_per_sec"));
    }

    @Test
    void testIndexingMetricsLogging() throws Exception {
        // Setup test appender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        ch.qos.logback.classic.Logger logger = 
            (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(IndexingMetrics.class);
        logger.addAppender(listAppender);
        logger.setLevel(Level.INFO); // Ensure INFO level is enabled

        // Create and use IndexingMetrics
        IndexingMetrics metrics = new IndexingMetrics();
        metrics.recordProcessingTime(100);
        metrics.incrementDocumentsProcessed();
        metrics.addNgramsGenerated(5);
        metrics.recordStateVerification("ngram_generation", true);
        metrics.recordStateVerification("position_merge", false);
        
        // Log metrics
        metrics.logMetrics("test_phase");

        // Verify log output - should always have summary metrics
        assertFalse(listAppender.list.isEmpty());
        String logMessage = listAppender.list.get(0).getMessage();
        JsonNode json = MAPPER.readTree(logMessage);

        assertEquals("indexing_summary", json.get("event").asText());
        assertEquals("test_phase", json.get("phase").asText());
        assertEquals(1, json.get("documents_processed").asInt());
        assertEquals(5, json.get("ngrams_generated").asInt());
    }

    @Test
    void testBatchStatsPerformance() {
        BatchStats stats = new BatchStats();
        long startTime = System.nanoTime();
        
        // Simulate processing 10000 documents
        for (int i = 0; i < 10000; i++) {
            stats.recordDocumentProcessing(1_000_000); // 1ms per document
        }
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Verify performance impact is minimal (should take less than 100ms for 10k operations)
        assertTrue(duration < 100_000_000, 
            "BatchStats overhead too high: " + (duration / 1_000_000.0) + "ms");
    }

    @Test
    void testIndexingMetricsPerformance() {
        IndexingMetrics metrics = new IndexingMetrics();
        long startTime = System.nanoTime();
        
        // Simulate intensive metrics recording
        for (int i = 0; i < 10000; i++) {
            metrics.recordProcessingTime(1_000_000);
            metrics.incrementDocumentsProcessed();
            metrics.addNgramsGenerated(3);
            metrics.recordStateVerification("test_state", i % 2 == 0);
        }
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Verify performance impact is minimal (should take less than 100ms for 40k operations)
        assertTrue(duration < 100_000_000, 
            "IndexingMetrics overhead too high: " + (duration / 1_000_000.0) + "ms");
    }
}
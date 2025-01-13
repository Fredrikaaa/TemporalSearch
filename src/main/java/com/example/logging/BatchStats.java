package com.example.logging;

import org.slf4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Tracks statistics for batch processing operations.
 * Provides JSON-formatted logging of batch processing results.
 */
public class BatchStats {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    private int totalDocuments;
    private int documentsWithNulls;
    private int documentsWithErrors;
    private final long startTime;
    
    public BatchStats() {
        this.startTime = System.currentTimeMillis();
    }
    
    public void incrementTotal() {
        totalDocuments++;
    }
    
    public void incrementNulls() {
        documentsWithNulls++;
    }
    
    public void incrementErrors() {
        documentsWithErrors++;
    }
    
    /**
     * Logs batch processing statistics in JSON format.
     *
     * @param logger The SLF4J logger to use
     */
    public void logBatchSummary(Logger logger) {
        try {
            ObjectNode json = MAPPER.createObjectNode()
                .put("event", "batch_complete")
                .put("total", totalDocuments)
                .put("nulls", documentsWithNulls)
                .put("errors", documentsWithErrors)
                .put("duration_ms", System.currentTimeMillis() - startTime);
            
            logger.info(json.toString());
        } catch (Exception e) {
            // Fallback to simple logging if JSON creation fails
            logger.info("Batch complete - total: {}, nulls: {}, errors: {}, duration_ms: {}",
                totalDocuments, documentsWithNulls, documentsWithErrors,
                System.currentTimeMillis() - startTime);
        }
    }
    
    public int getTotalDocuments() {
        return totalDocuments;
    }
    
    public int getDocumentsWithNulls() {
        return documentsWithNulls;
    }
    
    public int getDocumentsWithErrors() {
        return documentsWithErrors;
    }
    
    public long getDurationMs() {
        return System.currentTimeMillis() - startTime;
    }
} 
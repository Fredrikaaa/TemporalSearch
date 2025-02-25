package com.example.query.snippet;

/**
 * Represents a point in a document around which to expand context
 */
public record ContextAnchor(
    long documentId,
    int sentenceId,
    int tokenPosition,  // Optional, for highlighting specific tokens
    String variableName // The query variable this anchor represents
) {
    public ContextAnchor {
        if (documentId < 0) {
            throw new IllegalArgumentException("documentId must be non-negative");
        }
        if (sentenceId < 0) {
            throw new IllegalArgumentException("sentenceId must be non-negative");
        }
    }
} 
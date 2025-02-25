package com.example.query.model;

/**
 * Represents a SNIPPET expression in the SELECT clause of a query.
 * This node captures the variable to extract a snippet for and the optional window size.
 */
public record SnippetNode(
    String variable,
    int windowSize
) implements SelectColumn {
    
    public static final int DEFAULT_WINDOW_SIZE = 1;  // Default 1 sentence window
    
    public SnippetNode {
        if (variable == null || variable.isEmpty()) {
            throw new IllegalArgumentException("variable must not be null or empty");
        }
        if (windowSize < 1) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
    }

    @Override
    public String toString() {
        return String.format("SNIPPET(%s, window=%d)", variable, windowSize);
    }
} 
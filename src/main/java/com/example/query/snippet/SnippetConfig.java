package com.example.query.snippet;

/**
 * Configuration for snippet generation
 */
public record SnippetConfig(
    int windowSize,     // Number of sentences around match (0-10)
    String highlightStyle,  // Style for highlighting matches
    boolean showSentenceBoundaries  // Whether to show sentence boundaries
) {
    public static final SnippetConfig DEFAULT = new SnippetConfig(3, "**", false);

    public SnippetConfig {
        if (windowSize < 0 || windowSize > 10) {
            throw new IllegalArgumentException("windowSize must be between 0 and 10");
        }
        if (highlightStyle == null || highlightStyle.isEmpty()) {
            throw new IllegalArgumentException("highlightStyle must not be null or empty");
        }
    }
} 
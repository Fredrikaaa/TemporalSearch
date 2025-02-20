package com.example.snippet;

/**
 * Configuration for snippet generation
 */
public record SnippetConfig(
    int contextChars,    // Number of characters to include around match
    int maxLength        // Maximum total length of snippet
) {
    public static final SnippetConfig DEFAULT = new SnippetConfig(50, 200);
    
    public SnippetConfig {
        if (contextChars < 0) {
            throw new IllegalArgumentException("contextChars must be non-negative");
        }
        if (maxLength < contextChars * 2) {
            throw new IllegalArgumentException("maxLength must be at least twice contextChars");
        }
    }
} 
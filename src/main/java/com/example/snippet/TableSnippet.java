package com.example.snippet;

import java.util.List;
import java.util.Map;

/**
 * Represents a formatted text snippet for table display with highlights and extracted values
 */
public record TableSnippet(
    String text,
    List<SimpleSpan> highlights,
    Map<String, String> matchValues
) {
    /**
     * Represents a highlighted span in the text with type and value information
     */
    public record SimpleSpan(
        String type,     // Match category (e.g. PERSON, DATE)
        String value,    // Extracted value
        int start,      // Start position in text
        int end         // End position in text
    ) {
        public SimpleSpan {
            if (start < 0) {
                throw new IllegalArgumentException("start must be non-negative");
            }
            if (end < start) {
                throw new IllegalArgumentException("end must be greater than or equal to start");
            }
            if (type == null || type.isEmpty()) {
                throw new IllegalArgumentException("type must not be null or empty");
            }
        }
    }
} 
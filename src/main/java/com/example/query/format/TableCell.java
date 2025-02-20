package com.example.query.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a single cell in the result table with formatting metadata.
 */
public record TableCell(
    String rawValue,       // Original cell value
    String displayValue,   // Formatted value for display
    List<String> highlights, // Highlight markers in value
    int width             // Display width of content
) {
    /**
     * Creates a new table cell with validation.
     */
    public TableCell {
        Objects.requireNonNull(rawValue, "Raw value cannot be null");
        Objects.requireNonNull(displayValue, "Display value cannot be null");
        
        // Defensive copy of highlights list
        highlights = highlights != null ? 
            new ArrayList<>(highlights) : new ArrayList<>();

        if (width < 0) {
            throw new IllegalArgumentException("Cell width must be non-negative");
        }
    }

    /**
     * Creates a simple table cell without highlights.
     * @param value The cell value
     * @return A new TableCell with the given value
     */
    public static TableCell of(String value) {
        String safeValue = value != null ? value : "";
        return new TableCell(
            safeValue,
            safeValue,
            new ArrayList<>(),
            safeValue.length()
        );
    }

    /**
     * Creates a table cell with highlights.
     * @param value The cell value
     * @param highlights List of highlight markers
     * @return A new TableCell with the given value and highlights
     */
    public static TableCell withHighlights(String value, List<String> highlights) {
        String safeValue = value != null ? value : "";
        return new TableCell(
            safeValue,
            safeValue,
            highlights,
            safeValue.length()
        );
    }
} 
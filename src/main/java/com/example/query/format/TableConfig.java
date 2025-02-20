package com.example.query.format;

import java.util.Objects;

/**
 * Configuration for table formatting and output generation.
 */
public record TableConfig(
    int previewRows,        // Number of rows for console preview
    String outputFile,      // Optional output file path
    boolean showHeaders,    // Whether to include column headers
    int maxCellWidth,      // Maximum width for preview cells
    char delimiter         // CSV delimiter character
) {
    /**
     * Creates a new table configuration with validation.
     */
    public TableConfig {
        if (previewRows < 0) {
            throw new IllegalArgumentException("Preview rows must be non-negative");
        }
        if (maxCellWidth < 1) {
            throw new IllegalArgumentException("Max cell width must be positive");
        }
        Objects.requireNonNull(outputFile, "Output file path cannot be null");
    }

    /**
     * Creates a default table configuration.
     * @return A new TableConfig with default values
     */
    public static TableConfig getDefault() {
        return new TableConfig(
            10,           // Default preview rows
            "output.csv", // Default output file
            true,        // Show headers by default
            50,          // Default max cell width
            ','         // Default CSV delimiter
        );
    }
} 
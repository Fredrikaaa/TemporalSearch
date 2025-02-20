package com.example.query.model.table;

/**
 * Configuration for table formatting and display.
 */
public record TableConfig(
    int maxWidth,         // Maximum width for console display
    int previewRows,      // Number of rows to show in preview
    String outputFile,    // Optional CSV output file path
    boolean showHeaders   // Whether to show column headers
) {
    public static final TableConfig DEFAULT = new TableConfig(120, 10, null, true);
    
    public TableConfig {
        if (maxWidth < 20) {
            throw new IllegalArgumentException("maxWidth must be at least 20");
        }
        if (previewRows < 1) {
            throw new IllegalArgumentException("previewRows must be at least 1");
        }
    }
} 
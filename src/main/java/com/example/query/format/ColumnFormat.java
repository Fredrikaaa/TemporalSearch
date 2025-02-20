package com.example.query.format;

import java.util.Objects;
import java.util.function.Function;

/**
 * Formatting specification for a single column in the result table.
 */
public record ColumnFormat(
    String name,           // Column name from spec
    String alias,          // Display name
    int width,            // Display width for preview
    TextAlign align,      // Column alignment
    Function<String, String> formatter // Value formatting function
) {
    /**
     * Text alignment options for column values.
     */
    public enum TextAlign {
        LEFT,
        RIGHT,
        CENTER
    }

    /**
     * Creates a new column format with validation.
     */
    public ColumnFormat {
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(align, "Text alignment cannot be null");
        Objects.requireNonNull(formatter, "Value formatter cannot be null");
        
        if (width < 1) {
            throw new IllegalArgumentException("Column width must be positive");
        }

        // Use name as alias if none provided
        if (alias == null) {
            alias = name;
        }
    }

    /**
     * Creates a default left-aligned column format.
     * @param name The column name
     * @param width The display width
     * @return A new ColumnFormat with default settings
     */
    public static ColumnFormat createDefault(String name, int width) {
        return new ColumnFormat(
            name,
            null,
            width,
            TextAlign.LEFT,
            value -> value // Identity formatter
        );
    }
} 
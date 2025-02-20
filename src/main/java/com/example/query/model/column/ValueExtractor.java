package com.example.query.model.column;

import java.util.List;

/**
 * Interface for extracting values from index matches based on column specifications.
 */
public interface ValueExtractor {
    /**
     * Extracts values from the given match context.
     *
     * @param context The match context containing relevant data
     * @param spec The column specification defining what to extract
     * @return A list of extracted values, may be empty but never null
     * @throws IllegalArgumentException if the extraction configuration is invalid
     */
    List<String> extract(MatchContext context, ColumnSpec spec);

    /**
     * Formats a single extracted value according to column options.
     *
     * @param value The value to format
     * @param spec The column specification containing format options
     * @return The formatted value
     */
    String formatValue(String value, ColumnSpec spec);

    /**
     * Validates if this extractor can handle the given column type.
     *
     * @param type The column type to check
     * @return true if this extractor can handle the type
     */
    boolean supportsType(ColumnType type);

    /**
     * Gets the default value when extraction fails or no value is found.
     *
     * @param spec The column specification
     * @return The default value, usually empty string or N/A
     */
    default String getDefaultValue(ColumnSpec spec) {
        return "";
    }
} 
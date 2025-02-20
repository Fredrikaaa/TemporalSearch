package com.example.query.model.value;

import com.example.index.Position;
import com.example.query.model.column.ColumnSpec;

import java.util.List;
import java.util.Set;

/**
 * Interface for extracting values from index matches based on column specifications.
 */
public interface ValueExtractor {
    /**
     * Extracts values from the given document and match positions.
     * @param docId The document ID
     * @param matches The set of match positions
     * @param spec The column specification defining what to extract
     * @return A list of extracted values, may be empty but never null
     * @throws IllegalArgumentException if the extraction configuration is invalid
     */
    List<String> extract(Integer docId, Set<Position> matches, ColumnSpec spec);

    /**
     * Validates if this extractor can handle the given column type.
     * @param spec The column specification to check
     * @return true if this extractor can handle the column type
     */
    boolean supportsSpec(ColumnSpec spec);

    /**
     * Gets the default value when extraction fails or no value is found.
     * @param spec The column specification
     * @return The default value, usually empty string or N/A
     */
    default String getDefaultValue(ColumnSpec spec) {
        return "";
    }
} 
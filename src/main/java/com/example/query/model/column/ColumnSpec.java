package com.example.query.model.column;

import java.util.Map;
import java.util.Objects;

/**
 * Specification for a column in the query result table.
 *
 * @param name The unique identifier for the column
 * @param type The type of value to extract
 * @param alias Optional display name for the column
 * @param options Optional column-specific configuration options
 */
public record ColumnSpec(
    String name,
    ColumnType type,
    String alias,
    Map<String, String> options
) {
    /**
     * Creates a new column specification with validation.
     *
     * @throws NullPointerException if name or type is null
     */
    public ColumnSpec {
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(type, "Column type cannot be null");
    }
} 
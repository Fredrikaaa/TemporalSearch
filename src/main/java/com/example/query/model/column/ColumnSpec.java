package com.example.query.model.column;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Specification for a column in query results.
 */
public record ColumnSpec(
    String name,
    ColumnType type,
    String alias,
    Map<String, String> options
) {
    /**
     * Creates a new column specification with validation.
     */
    public ColumnSpec {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(type, "type must not be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        // Make defensive copy of options if not null
        options = options != null ? Map.copyOf(options) : null;
    }

    /**
     * Creates a new column specification.
     * @param name The column name
     * @param type The column type
     */
    public ColumnSpec(String name, ColumnType type) {
        this(name, type, null, null);
    }
    
    /**
     * Creates a new column specification with options.
     * @param name The column name
     * @param type The column type
     * @param options Additional options for the column
     */
    public ColumnSpec(String name, ColumnType type, Map<String, String> options) {
        this(name, type, null, options);
    }
    
    /**
     * Gets the column alias.
     */
    public String alias() {
        return alias;
    }
    
    /**
     * Gets the column options.
     */
    public Map<String, String> options() {
        return options != null ? new HashMap<>(options) : null;
    }
    
    /**
     * Gets a specific option value.
     * @param key The option key
     * @return The option value, or null if not set
     */
    public String getOption(String key) {
        return options != null ? options.get(key) : null;
    }
    
    /**
     * Sets an option value.
     * @param key The option key
     * @param value The option value
     * @return A new ColumnSpec with the updated option
     */
    public ColumnSpec withOption(String key, String value) {
        Map<String, String> newOptions = options != null ? new HashMap<>(options) : new HashMap<>();
        newOptions.put(key, value);
        return new ColumnSpec(name, type, alias, newOptions);
    }
    
    @Override
    public String toString() {
        return String.format("%s(%s)", name, type);
    }
} 
package com.example.query.format;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for table formatting options.
 * This class provides display settings for result tables.
 */
public record TableConfig(
    Map<String, String> formatOptions,
    boolean showHeaders,
    String nullValueDisplay,
    int maxColumnWidth
) {
    // Default values
    private static final boolean DEFAULT_SHOW_HEADERS = true;
    private static final String DEFAULT_NULL_VALUE = "NULL";
    private static final int DEFAULT_MAX_WIDTH = 50;
    
    // Default instance
    private static final TableConfig DEFAULT = new TableConfig(
        Map.of(), DEFAULT_SHOW_HEADERS, DEFAULT_NULL_VALUE, DEFAULT_MAX_WIDTH);
    
    /**
     * Creates a table config with validation of parameters.
     */
    public TableConfig {
        // Defensive copy of the map
        formatOptions = formatOptions == null ? 
            Map.of() : Map.copyOf(formatOptions);
            
        // Validate max column width
        if (maxColumnWidth <= 0) {
            throw new IllegalArgumentException("Maximum column width must be positive");
        }
        
        // Ensure null value display is not null
        if (nullValueDisplay == null) {
            nullValueDisplay = DEFAULT_NULL_VALUE;
        }
    }
    
    /**
     * Creates a new TableConfig with default values.
     */
    public TableConfig() {
        this(Map.of(), DEFAULT_SHOW_HEADERS, DEFAULT_NULL_VALUE, DEFAULT_MAX_WIDTH);
    }
    
    /**
     * Gets the default table configuration.
     *
     * @return The default table configuration
     */
    public static TableConfig getDefault() {
        return DEFAULT;
    }
    
    /**
     * Creates a compact table configuration with no headers and smaller width.
     *
     * @return A compact table configuration
     */
    public static TableConfig compact() {
        return new TableConfig(Map.of(), false, "-", 30);
    }
    
    /**
     * Creates a wide table configuration with larger column width.
     *
     * @return A wide table configuration
     */
    public static TableConfig wide() {
        return new TableConfig(Map.of(), true, DEFAULT_NULL_VALUE, 100);
    }
    
    /**
     * Gets a format option value.
     *
     * @param key The option key
     * @return The option value, or null if not present
     */
    public String getFormatOption(String key) {
        return formatOptions.get(key);
    }
    
    /**
     * Creates a new TableConfig with the specified format option.
     *
     * @param key The option key
     * @param value The option value
     * @return A new TableConfig with the updated option
     */
    public TableConfig withFormatOption(String key, String value) {
        Map<String, String> newOptions = new HashMap<>(formatOptions);
        newOptions.put(key, value);
        return new TableConfig(newOptions, showHeaders, nullValueDisplay, maxColumnWidth);
    }
    
    /**
     * Creates a new TableConfig with the specified headers setting.
     *
     * @param showHeaders Whether to show headers
     * @return A new TableConfig with the updated setting
     */
    public TableConfig withShowHeaders(boolean showHeaders) {
        return new TableConfig(formatOptions, showHeaders, nullValueDisplay, maxColumnWidth);
    }
    
    /**
     * Creates a new TableConfig with the specified null value display.
     *
     * @param nullValueDisplay The null value display string
     * @return A new TableConfig with the updated setting
     */
    public TableConfig withNullValueDisplay(String nullValueDisplay) {
        return new TableConfig(formatOptions, showHeaders, nullValueDisplay, maxColumnWidth);
    }
    
    /**
     * Creates a new TableConfig with the specified maximum column width.
     *
     * @param maxColumnWidth The maximum column width
     * @return A new TableConfig with the updated setting
     */
    public TableConfig withMaxColumnWidth(int maxColumnWidth) {
        return new TableConfig(formatOptions, showHeaders, nullValueDisplay, maxColumnWidth);
    }
} 
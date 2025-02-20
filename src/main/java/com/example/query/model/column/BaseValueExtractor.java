package com.example.query.model.column;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Base implementation of ValueExtractor with common functionality.
 */
public abstract class BaseValueExtractor implements ValueExtractor {
    private static final Logger logger = Logger.getLogger(BaseValueExtractor.class.getName());

    protected final ColumnType supportedType;

    /**
     * Creates a new base value extractor.
     *
     * @param supportedType The column type this extractor supports
     */
    protected BaseValueExtractor(ColumnType supportedType) {
        this.supportedType = supportedType;
    }

    @Override
    public boolean supportsType(ColumnType type) {
        return this.supportedType == type;
    }

    @Override
    public String formatValue(String value, ColumnSpec spec) {
        if (value == null || value.isEmpty()) {
            return getDefaultValue(spec);
        }

        Map<String, String> options = spec.options();
        if (options == null || !options.containsKey("format")) {
            return value;
        }

        return formatWithOptions(value, options);
    }

    /**
     * Format a value according to the options map.
     * Subclasses should override this to provide type-specific formatting.
     *
     * @param value The value to format
     * @param options The formatting options
     * @return The formatted value
     */
    protected String formatWithOptions(String value, Map<String, String> options) {
        return value;
    }

    /**
     * Helper method to safely return an empty list when extraction fails.
     *
     * @param context The match context
     * @param error The error message
     * @return An empty list
     */
    protected List<String> handleExtractionError(MatchContext context, String error) {
        logger.warning(String.format(
            "Failed to extract value for type %s: %s (match: '%s')",
            supportedType,
            error,
            context.matchedText()
        ));
        return Collections.emptyList();
    }
} 
package com.example.query.model.column;

import java.util.Map;
import java.util.Objects;

/**
 * Contains context information about a match for value extraction.
 */
public record MatchContext(
    String matchedText,
    int startOffset,
    int endOffset,
    Map<String, Object> metadata
) {
    /**
     * Creates a new match context with validation.
     *
     * @throws NullPointerException if matchedText is null
     */
    public MatchContext {
        Objects.requireNonNull(matchedText, "Matched text cannot be null");
        metadata = metadata != null ? metadata : Map.of();
    }

    /**
     * Gets a metadata value of the specified type.
     *
     * @param key The metadata key
     * @param type The expected value type
     * @return The value cast to the specified type, or null if not present
     * @throws ClassCastException if the value is not of the expected type
     */
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key, Class<T> type) {
        Object value = metadata.get(key);
        return value != null ? type.cast(value) : null;
    }
} 
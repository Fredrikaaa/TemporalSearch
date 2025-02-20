package com.example.query.model.value;

import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Factory for creating and managing value extractors.
 */
public class ValueExtractorFactory {
    private static final Logger logger = Logger.getLogger(ValueExtractorFactory.class.getName());
    private final List<ValueExtractor> extractors;

    /**
     * Creates a new value extractor factory with default extractors.
     */
    public ValueExtractorFactory() {
        extractors = new ArrayList<>();
        registerDefaultExtractors();
    }

    /**
     * Gets a value extractor that can handle the given column specification.
     * @param spec The column specification
     * @return A suitable value extractor, or null if none found
     */
    public ValueExtractor getExtractor(ColumnSpec spec) {
        return extractors.stream()
            .filter(e -> e.supportsSpec(spec))
            .findFirst()
            .orElse(null);
    }

    /**
     * Registers a new value extractor.
     * @param extractor The extractor to register
     */
    public void registerExtractor(ValueExtractor extractor) {
        extractors.add(extractor);
    }

    /**
     * Registers the default set of value extractors.
     */
    private void registerDefaultExtractors() {
        // Register built-in extractors
        registerExtractor(new PersonValueExtractor());
        registerExtractor(new SnippetValueExtractor());
        // TODO: Add more extractors as needed
    }

    /**
     * Gets all registered extractors.
     * @return The list of registered extractors
     */
    public List<ValueExtractor> getExtractors() {
        return new ArrayList<>(extractors);
    }

    /**
     * Gets all column types that have registered extractors.
     * @return The list of supported column types
     */
    public List<ColumnType> getSupportedTypes() {
        return extractors.stream()
            .filter(e -> e instanceof BaseValueExtractor)
            .map(e -> ((BaseValueExtractor) e).supportedType)
            .distinct()
            .toList();
    }
} 
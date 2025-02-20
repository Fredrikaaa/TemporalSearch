package com.example.query.model.value;

import com.example.index.Position;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Base implementation of ValueExtractor with common functionality.
 */
public abstract class BaseValueExtractor implements ValueExtractor {
    protected static final Logger logger = Logger.getLogger(BaseValueExtractor.class.getName());
    protected final ColumnType supportedType;

    /**
     * Creates a new base value extractor.
     * @param supportedType The column type this extractor supports
     */
    protected BaseValueExtractor(ColumnType supportedType) {
        this.supportedType = supportedType;
    }

    @Override
    public boolean supportsSpec(ColumnSpec spec) {
        return spec.type() == supportedType;
    }

    @Override
    public List<String> extract(Integer docId, Set<Position> matches, ColumnSpec spec) {
        if (!supportsSpec(spec)) {
            logger.warning("Attempted to use incompatible extractor for type: " + spec.type());
            return Collections.emptyList();
        }

        try {
            return extractValues(docId, matches, spec);
        } catch (Exception e) {
            logger.warning("Failed to extract values: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Template method for extracting values from matches.
     * Subclasses must implement this to provide type-specific extraction.
     * @param docId The document ID
     * @param matches The set of match positions
     * @param spec The column specification
     * @return A list of extracted values
     */
    protected abstract List<String> extractValues(Integer docId, Set<Position> matches, ColumnSpec spec);

    /**
     * Helper method to safely return an empty list when extraction fails.
     * @param error The error message to log
     * @return An empty list
     */
    protected List<String> handleError(String error) {
        logger.warning(error);
        return Collections.emptyList();
    }

    /**
     * Helper method to merge overlapping positions.
     * @param positions The positions to merge
     * @return A list of merged positions with no overlaps
     */
    protected List<Position> mergeOverlapping(Set<Position> positions) {
        if (positions.isEmpty()) {
            return Collections.emptyList();
        }

        List<Position> sorted = new ArrayList<>(positions);
        sorted.sort((a, b) -> a.start() - b.start());

        List<Position> merged = new ArrayList<>();
        Position current = sorted.get(0);

        for (int i = 1; i < sorted.size(); i++) {
            Position next = sorted.get(i);
            if (current.overlaps(next)) {
                current = current.merge(next);
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);

        return merged;
    }
} 
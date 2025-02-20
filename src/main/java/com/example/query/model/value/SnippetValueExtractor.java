package com.example.query.model.value;

import com.example.index.Position;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ValueExtractor implementation for extracting text snippets around matches.
 */
public class SnippetValueExtractor extends BaseValueExtractor {
    private static final int DEFAULT_CONTEXT_LENGTH = 50;
    private static final String CONTEXT_LENGTH_OPTION = "length";

    /**
     * Creates a new snippet value extractor.
     */
    public SnippetValueExtractor() {
        super(ColumnType.SNIPPET);
    }

    @Override
    protected List<String> extractValues(Integer docId, Set<Position> matches, ColumnSpec spec) {
        // Get the desired context length from options
        int contextLength = getContextLength(spec);

        // Merge overlapping positions to avoid duplicate snippets
        List<Position> mergedPositions = mergeOverlapping(matches);
        List<String> snippets = new ArrayList<>();

        for (Position pos : mergedPositions) {
            // Calculate snippet boundaries
            int snippetStart = Math.max(0, pos.start() - contextLength);
            int snippetEnd = pos.end() + contextLength;

            // TODO: Implement actual text extraction from SQLite
            // For now, just return placeholder values
            String placeholder = String.format(
                "...text[%d:%d] *match[%d:%d]* text[%d:%d]...",
                snippetStart, pos.start(),
                pos.start(), pos.end(),
                pos.end(), snippetEnd
            );
            snippets.add(placeholder);
        }

        return snippets;
    }

    @Override
    public String getDefaultValue(ColumnSpec spec) {
        return "No matching text";
    }

    /**
     * Gets the context length from column options or uses the default.
     * @param spec The column specification
     * @return The context length to use
     */
    private int getContextLength(ColumnSpec spec) {
        Map<String, String> options = spec.options();
        if (options != null && options.containsKey(CONTEXT_LENGTH_OPTION)) {
            try {
                return Integer.parseInt(options.get(CONTEXT_LENGTH_OPTION));
            } catch (NumberFormatException e) {
                // Log warning and use default
                logger.warning("Invalid context length option: " + options.get(CONTEXT_LENGTH_OPTION));
            }
        }
        return DEFAULT_CONTEXT_LENGTH;
    }
} 
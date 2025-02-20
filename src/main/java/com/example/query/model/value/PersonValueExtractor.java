package com.example.query.model.value;

import com.example.index.Position;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * ValueExtractor implementation for extracting person names from index matches.
 */
public class PersonValueExtractor extends BaseValueExtractor {
    private static final String PERSON_INDEX_KEY = "ner_person";

    /**
     * Creates a new person value extractor.
     */
    public PersonValueExtractor() {
        super(ColumnType.PERSON);
    }

    @Override
    protected List<String> extractValues(Integer docId, Set<Position> matches, ColumnSpec spec) {
        // Merge overlapping positions to avoid duplicate names
        List<Position> mergedPositions = mergeOverlapping(matches);
        List<String> names = new ArrayList<>();

        for (Position pos : mergedPositions) {
            // TODO: Implement actual name extraction from index
            // For now, just return placeholder values
            names.add("Person_" + pos.start() + "_" + pos.end());
        }

        return names;
    }

    @Override
    public String getDefaultValue(ColumnSpec spec) {
        return "Unknown Person";
    }
} 
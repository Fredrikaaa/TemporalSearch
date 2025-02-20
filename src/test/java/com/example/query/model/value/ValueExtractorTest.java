package com.example.query.model.value;

import com.example.index.Position;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ValueExtractorTest {
    private ValueExtractorFactory factory;
    private Set<Position> testPositions;
    private ColumnSpec personSpec;
    private ColumnSpec snippetSpec;

    @BeforeEach
    void setUp() {
        factory = new ValueExtractorFactory();
        
        // Create test positions
        testPositions = new HashSet<>();
        testPositions.add(Position.of(1, 10, 20));
        testPositions.add(Position.of(1, 15, 25)); // Overlaps with first
        testPositions.add(Position.of(1, 30, 40)); // Separate position

        // Create test column specs
        personSpec = new ColumnSpec("person", ColumnType.PERSON, null, null);
        Map<String, String> snippetOptions = new HashMap<>();
        snippetOptions.put("length", "30");
        snippetSpec = new ColumnSpec("snippet", ColumnType.SNIPPET, null, snippetOptions);
    }

    @Test
    void testPersonValueExtractor() {
        ValueExtractor extractor = factory.getExtractor(personSpec);
        assertNotNull(extractor);
        assertTrue(extractor instanceof PersonValueExtractor);
        assertTrue(extractor.supportsSpec(personSpec));

        List<String> values = extractor.extract(1, testPositions, personSpec);
        assertEquals(2, values.size()); // Should merge overlapping positions
    }

    @Test
    void testSnippetValueExtractor() {
        ValueExtractor extractor = factory.getExtractor(snippetSpec);
        assertNotNull(extractor);
        assertTrue(extractor instanceof SnippetValueExtractor);
        assertTrue(extractor.supportsSpec(snippetSpec));

        List<String> values = extractor.extract(1, testPositions, snippetSpec);
        assertEquals(2, values.size()); // Should merge overlapping positions
        
        // Verify snippet format
        for (String value : values) {
            assertTrue(value.contains("*match"));
            assertTrue(value.contains("text"));
        }
    }

    @Test
    void testMergeOverlappingPositions() {
        ValueExtractor extractor = factory.getExtractor(personSpec);
        List<String> values = extractor.extract(1, testPositions, personSpec);

        // Should merge overlapping positions (10-20 and 15-25)
        assertEquals(2, values.size());
    }

    @Test
    void testSnippetContextLength() {
        // Test with custom context length
        Map<String, String> options = new HashMap<>();
        options.put("length", "100");
        ColumnSpec customSpec = new ColumnSpec("snippet", ColumnType.SNIPPET, null, options);

        ValueExtractor extractor = factory.getExtractor(customSpec);
        List<String> values = extractor.extract(1, Set.of(Position.of(1, 50, 60)), customSpec);

        String snippet = values.get(0);
        assertTrue(snippet.contains("[0:50]")); // Should start at 0 since 50-100 would be negative
    }

    @Test
    void testInvalidContextLength() {
        // Test with invalid context length
        Map<String, String> options = new HashMap<>();
        options.put("length", "invalid");
        ColumnSpec invalidSpec = new ColumnSpec("snippet", ColumnType.SNIPPET, null, options);

        ValueExtractor extractor = factory.getExtractor(invalidSpec);
        List<String> values = extractor.extract(1, Set.of(Position.of(1, 50, 60)), invalidSpec);

        // Should use default length
        assertFalse(values.isEmpty());
    }

    @Test
    void testUnsupportedColumnType() {
        // Create a spec with unsupported type
        ColumnSpec unsupportedSpec = new ColumnSpec("test", ColumnType.DATE, null, null);
        
        ValueExtractor extractor = factory.getExtractor(unsupportedSpec);
        assertNull(extractor); // Should not find an extractor
    }

    @Test
    void testGetSupportedTypes() {
        List<ColumnType> types = factory.getSupportedTypes();
        assertTrue(types.contains(ColumnType.PERSON));
        assertTrue(types.contains(ColumnType.SNIPPET));
        assertEquals(2, types.size());
    }

    @Test
    void testCustomExtractor() {
        // Test registering a custom extractor
        ValueExtractor customExtractor = new BaseValueExtractor(ColumnType.DATE) {
            @Override
            protected List<String> extractValues(Integer docId, Set<Position> matches, ColumnSpec spec) {
                return List.of("2024-01-01");
            }
        };

        factory.registerExtractor(customExtractor);
        
        ColumnSpec dateSpec = new ColumnSpec("date", ColumnType.DATE, null, null);
        ValueExtractor extractor = factory.getExtractor(dateSpec);
        
        assertNotNull(extractor);
        assertEquals(customExtractor, extractor);
    }
} 
package com.example.query.model.column;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

class ValueExtractorTest {

    private static class TestExtractor extends BaseValueExtractor {
        TestExtractor() {
            super(ColumnType.PERSON);
        }

        @Override
        public List<String> extract(MatchContext context, ColumnSpec spec) {
            return List.of(context.matchedText().toUpperCase());
        }

        @Override
        protected String formatWithOptions(String value, Map<String, String> options) {
            String format = options.get("format");
            return "lower".equals(format) ? value.toLowerCase() : value;
        }
    }

    @Test
    void testExtraction() {
        ValueExtractor extractor = new TestExtractor();
        MatchContext context = new MatchContext("John Doe", 0, 8, null);
        ColumnSpec spec = new ColumnSpec("name", ColumnType.PERSON, null, null);

        List<String> result = extractor.extract(context, spec);
        assertEquals(List.of("JOHN DOE"), result);
    }

    @Test
    void testFormatting() {
        ValueExtractor extractor = new TestExtractor();
        ColumnSpec spec = new ColumnSpec(
            "name",
            ColumnType.PERSON,
            null,
            Map.of("format", "lower")
        );

        String result = extractor.formatValue("JOHN DOE", spec);
        assertEquals("john doe", result);
    }

    @Test
    void testSupportsType() {
        ValueExtractor extractor = new TestExtractor();
        assertTrue(extractor.supportsType(ColumnType.PERSON));
        assertFalse(extractor.supportsType(ColumnType.DATE));
    }

    @Test
    void testDefaultValue() {
        ValueExtractor extractor = new TestExtractor();
        ColumnSpec spec = new ColumnSpec("name", ColumnType.PERSON, null, null);

        String result = extractor.formatValue("", spec);
        assertEquals("", result);

        result = extractor.formatValue(null, spec);
        assertEquals("", result);
    }

    @Test
    void testMatchContextMetadata() {
        Map<String, Object> metadata = Map.of(
            "confidence", 0.95,
            "source", "ner"
        );
        
        MatchContext context = new MatchContext("John Doe", 0, 8, metadata);
        
        assertEquals(0.95, context.getMetadata("confidence", Double.class));
        assertEquals("ner", context.getMetadata("source", String.class));
        assertNull(context.getMetadata("missing", String.class));
    }
} 
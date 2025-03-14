package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;

/**
 * Tests for the AnnotationEntry class.
 */
class AnnotationEntryTest extends BaseIndexTest {

    @Test
    void testBasicIndexing() throws Exception {
        // Insert test document
        LocalDate timestamp = LocalDate.now();
        TestData.insertDocument(sqliteConn, 1, timestamp);

        // Insert test annotations
        TestData.insertBasicAnnotations(sqliteConn);

        // Create and verify an annotation entry
        AnnotationEntry entry = TestData.createAnnotation(1, "cat", "NOUN");
        assertEquals(1, entry.getDocumentId());
        assertEquals(1, entry.getSentenceId());
        assertEquals(0, entry.getBeginChar());
        assertEquals(3, entry.getEndChar());
        assertEquals("cat", entry.getLemma());
        assertEquals("NOUN", entry.getPos());
        assertNotNull(entry.getTimestamp());
    }

    @Test
    void testNullHandling() {
        // Test with null lemma and POS
        AnnotationEntry entry = new AnnotationEntry(1, 1, 1, 0, 3, null, null, LocalDate.now());
        assertNull(entry.getLemma());
        assertNull(entry.getPos());
    }

    @Test
    void testEmptyStrings() {
        // Test with empty strings
        AnnotationEntry entry = new AnnotationEntry(1, 1, 1, 0, 3, "", "", LocalDate.now());
        assertEquals("", entry.getLemma());
        assertEquals("", entry.getPos());
    }

    @Test
    void testTimestampHandling() {
        LocalDate timestamp = LocalDate.of(2024, 1, 20);
        AnnotationEntry entry = new AnnotationEntry(1, 1, 1, 0, 3, "test", "NOUN", timestamp);
        assertEquals(timestamp, entry.getTimestamp());
    }
    
    @Test
    void testAnnotationId() {
        // Test annotation ID getter
        int annotationId = 42;
        AnnotationEntry entry = new AnnotationEntry(annotationId, 1, 1, 0, 3, "test", "NOUN", LocalDate.now());
        assertEquals(annotationId, entry.getAnnotationId());
    }
} 
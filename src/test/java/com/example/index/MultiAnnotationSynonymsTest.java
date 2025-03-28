package com.example.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MultiAnnotationSynonyms class.
 */
public class MultiAnnotationSynonymsTest {
    
    @TempDir
    Path tempDir;
    
    private MultiAnnotationSynonyms synonyms;
    
    @BeforeEach
    public void setUp() throws Exception {
        synonyms = new MultiAnnotationSynonyms(tempDir);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (synonyms != null) {
            synonyms.close();
            synonyms = null;
        }
    }
    
    @Test
    public void testDateSynonyms() {
        String date1 = "2023-01-01";
        String date2 = "2022-12-25";
        
        int id1 = synonyms.getOrCreateId(date1, AnnotationType.DATE);
        int id2 = synonyms.getOrCreateId(date2, AnnotationType.DATE);
        
        // IDs should be unique
        assertNotEquals(id1, id2);
        
        // Getting the same value should return the same ID
        assertEquals(id1, synonyms.getOrCreateId(date1, AnnotationType.DATE));
        assertEquals(id2, synonyms.getOrCreateId(date2, AnnotationType.DATE));
        
        // Looking up by ID should return the original value
        assertEquals(date1, synonyms.getValue(id1, AnnotationType.DATE));
        assertEquals(date2, synonyms.getValue(id2, AnnotationType.DATE));
        
        // Size should reflect the number of entries
        assertEquals(2, synonyms.size(AnnotationType.DATE));
    }
    
    @Test
    public void testNerSynonyms() {
        String ner1 = "PERSON";
        String ner2 = "LOCATION";
        
        int id1 = synonyms.getOrCreateId(ner1, AnnotationType.NER);
        int id2 = synonyms.getOrCreateId(ner2, AnnotationType.NER);
        
        // IDs should be unique
        assertNotEquals(id1, id2);
        
        // Getting the same value should return the same ID
        assertEquals(id1, synonyms.getOrCreateId(ner1, AnnotationType.NER));
        assertEquals(id2, synonyms.getOrCreateId(ner2, AnnotationType.NER));
        
        // Looking up by ID should return the original value
        assertEquals(ner1, synonyms.getValue(id1, AnnotationType.NER));
        assertEquals(ner2, synonyms.getValue(id2, AnnotationType.NER));
        
        // Size should reflect the number of entries
        assertEquals(2, synonyms.size(AnnotationType.NER));
    }
    
    @Test
    public void testPosSynonyms() {
        String pos1 = "NN";
        String pos2 = "VB";
        
        int id1 = synonyms.getOrCreateId(pos1, AnnotationType.POS);
        int id2 = synonyms.getOrCreateId(pos2, AnnotationType.POS);
        
        // IDs should be unique
        assertNotEquals(id1, id2);
        
        // Getting the same value should return the same ID
        assertEquals(id1, synonyms.getOrCreateId(pos1, AnnotationType.POS));
        assertEquals(id2, synonyms.getOrCreateId(pos2, AnnotationType.POS));
        
        // Looking up by ID should return the original value
        assertEquals(pos1, synonyms.getValue(id1, AnnotationType.POS));
        assertEquals(pos2, synonyms.getValue(id2, AnnotationType.POS));
        
        // Size should reflect the number of entries
        assertEquals(2, synonyms.size(AnnotationType.POS));
    }
    
    @Test
    public void testDependencySynonyms() {
        String dep1 = "nsubj";
        String dep2 = "dobj";
        
        int id1 = synonyms.getOrCreateId(dep1, AnnotationType.DEPENDENCY);
        int id2 = synonyms.getOrCreateId(dep2, AnnotationType.DEPENDENCY);
        
        // IDs should be unique
        assertNotEquals(id1, id2);
        
        // Getting the same value should return the same ID
        assertEquals(id1, synonyms.getOrCreateId(dep1, AnnotationType.DEPENDENCY));
        assertEquals(id2, synonyms.getOrCreateId(dep2, AnnotationType.DEPENDENCY));
        
        // Looking up by ID should return the original value
        assertEquals(dep1, synonyms.getValue(id1, AnnotationType.DEPENDENCY));
        assertEquals(dep2, synonyms.getValue(id2, AnnotationType.DEPENDENCY));
        
        // Size should reflect the number of entries
        assertEquals(2, synonyms.size(AnnotationType.DEPENDENCY));
    }
    
    @Test
    public void testTotalSize() {
        // Add some synonyms
        synonyms.getOrCreateId("2023-01-01", AnnotationType.DATE);
        synonyms.getOrCreateId("PERSON", AnnotationType.NER);
        synonyms.getOrCreateId("NN", AnnotationType.POS);
        synonyms.getOrCreateId("nsubj", AnnotationType.DEPENDENCY);
        
        // Size should reflect the total number of entries
        assertEquals(4, synonyms.size());
        assertEquals(1, synonyms.size(AnnotationType.DATE));
        assertEquals(1, synonyms.size(AnnotationType.NER));
        assertEquals(1, synonyms.size(AnnotationType.POS));
        assertEquals(1, synonyms.size(AnnotationType.DEPENDENCY));
    }
    
    @Test
    public void testNamespaceIsolation() {
        // Same string used for different annotation types
        String value = "test";
        
        // Get IDs for the same string but different types
        int id1 = synonyms.getOrCreateId(value, AnnotationType.NER);
        int id2 = synonyms.getOrCreateId(value, AnnotationType.POS);
        int id3 = synonyms.getOrCreateId(value, AnnotationType.DEPENDENCY);
        
        // IDs should all be different because they're in different namespaces
        assertNotEquals(id1, id2);
        assertNotEquals(id1, id3);
        assertNotEquals(id2, id3);
        
        // Looking up by ID should return the correct value in the correct namespace
        assertEquals(value, synonyms.getValue(id1, AnnotationType.NER));
        assertEquals(value, synonyms.getValue(id2, AnnotationType.POS));
        assertEquals(value, synonyms.getValue(id3, AnnotationType.DEPENDENCY));
        
        // Looking up with wrong type should return null
        assertNull(synonyms.getValue(id1, AnnotationType.POS));
        assertNull(synonyms.getValue(id2, AnnotationType.NER));
    }
    
    @Test
    public void testInvalidDates() {
        // Invalid formats
        assertThrows(IllegalArgumentException.class, () -> 
            synonyms.getOrCreateId("not-a-date", AnnotationType.DATE));
        assertThrows(IllegalArgumentException.class, () -> 
            synonyms.getOrCreateId("01/01/2023", AnnotationType.DATE));
        
        // Valid format but invalid date
        assertThrows(IllegalArgumentException.class, () -> 
            synonyms.getOrCreateId("2023-02-30", AnnotationType.DATE));
    }
} 
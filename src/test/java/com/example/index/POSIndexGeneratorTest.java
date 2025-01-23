package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.time.LocalDate;
import java.nio.file.*;
import java.util.*;

/**
 * Tests for the POSIndexGenerator class.
 */
class POSIndexGeneratorTest extends BaseIndexTest {
    private Path indexPath;
    private Path stopwordsPath;
    
    @BeforeEach
    @Override
    void setUp() throws Exception {
        super.setUp();
        indexPath = tempDir.resolve("test-pos-index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an");
        Files.write(stopwordsPath, stopwords);
        
        // Insert test data
        setupTestData();
    }
    
    private void setupTestData() throws SQLException {
        // Insert test document
        LocalDate timestamp = LocalDate.now();
        TestData.insertDocument(sqliteConn, 1, timestamp);
        
        // Insert test annotations
        try (PreparedStatement stmt = sqliteConn.prepareStatement(
            "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) VALUES (?, ?, ?, ?, ?, ?)"
        )) {
            Object[][] data = {
                {1, 1, 0, 3, "cat", "NOUN"},
                {1, 1, 4, 10, "chases", "VERB"},
                {1, 1, 11, 16, "mouse", "NOUN"},
                {1, 1, 17, 20, "the", "DET"},  // Stopword but should be indexed since POS doesn't filter
                {1, 1, 21, 25, null, "PUNCT"}  // Null lemma but valid POS
            };
            
            for (Object[] row : data) {
                for (int i = 0; i < row.length; i++) {
                    stmt.setObject(i + 1, row[i]);
                }
                stmt.executeUpdate();
            }
        }
    }
    
    @Test
    void testBasicIndexing() throws Exception {
        // Generate index
        try (POSIndexGenerator generator = new POSIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check that all POS tags are indexed
            String[] expectedTags = {"noun", "verb", "det", "punct"};
            for (String tag : expectedTags) {
                byte[] value = db.get(bytes(tag));
                assertNotNull(value, "POS tag " + tag + " should be indexed");
                
                PositionList positions = PositionList.deserialize(value);
                assertTrue(positions.size() > 0, "Should have positions for " + tag);
            }
            
            // Verify NOUN has two positions
            PositionList nounPositions = PositionList.deserialize(db.get(bytes("noun")));
            assertEquals(2, nounPositions.size(), "Should have two NOUN positions");
            
            // Verify DET is indexed despite being a stopword
            PositionList detPositions = PositionList.deserialize(db.get(bytes("det")));
            assertEquals(1, detPositions.size(), "Should have one DET position");
        }
    }
    
    @Test
    void testCaseNormalization() throws Exception {
        // Insert mixed case POS tags
        try (PreparedStatement stmt = sqliteConn.prepareStatement(
            "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) VALUES (?, ?, ?, ?, ?, ?)"
        )) {
            Object[][] data = {
                {1, 2, 0, 3, "dog", "Noun"},
                {1, 2, 4, 9, "barks", "VERB"},
                {1, 2, 10, 14, "loud", "ADJ"}
            };
            
            for (Object[] row : data) {
                for (int i = 0; i < row.length; i++) {
                    stmt.setObject(i + 1, row[i]);
                }
                stmt.executeUpdate();
            }
        }
        
        // Generate index
        try (POSIndexGenerator generator = new POSIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }
        
        // Verify case normalization
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check that mixed case tags are normalized
            byte[] nounValue = db.get(bytes("noun"));
            assertNotNull(nounValue, "noun should be indexed");
            
            byte[] verbValue = db.get(bytes("verb"));
            assertNotNull(verbValue, "verb should be indexed");
            
            // Original case versions should not exist
            assertNull(db.get(bytes("Noun")), "Original case version should not exist");
            assertNull(db.get(bytes("VERB")), "Original case version should not exist");
        }
    }
    
    @Test
    void testNullAndEmptyHandling() throws Exception {
        // Insert null and empty POS tags
        try (PreparedStatement stmt = sqliteConn.prepareStatement(
            "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) VALUES (?, ?, ?, ?, ?, ?)"
        )) {
            Object[][] data = {
                {1, 2, 0, 3, "test", null},
                {1, 2, 4, 8, "test", ""},
                {1, 2, 9, 13, "test", " "}
            };
            
            for (Object[] row : data) {
                for (int i = 0; i < row.length; i++) {
                    stmt.setObject(i + 1, row[i]);
                }
                stmt.executeUpdate();
            }
        }
        
        // Generate index
        try (POSIndexGenerator generator = new POSIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }
        
        // Verify null and empty tags are not indexed
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            assertNull(db.get(bytes("")), "Empty string should not be indexed");
            assertNull(db.get(bytes(" ")), "Whitespace should not be indexed");
        }
    }
} 
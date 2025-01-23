package com.example.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class NerDateIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-ner.txt";
    private NerDateIndexGenerator indexer;

    @Override
    protected void createBasicTables() throws Exception {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("""
                CREATE TABLE documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL
                )
            """);

            stmt.execute("""
                CREATE TABLE annotations (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    begin_char INTEGER,
                    end_char INTEGER,
                    ner TEXT,
                    normalized_ner TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test documents
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-15T10:00:00Z')");
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (2, '2024-01-16T10:00:00Z')");
            
            // Insert test annotations
            stmt.execute(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) VALUES " +
                "(1, 1, 0, 10, 'DATE', '2024-01-20'), " +  // Valid date
                "(1, 2, 15, 25, 'DATE', '2024-02-01'), " + // Valid date
                "(2, 1, 5, 15, 'DATE', '2024-01-15'), " +  // Valid date
                "(2, 2, 20, 30, 'DATE', null), " +         // Invalid - null date
                "(2, 3, 35, 45, 'DATE', 'invalid-date'), " + // Invalid format
                "(2, 4, 50, 60, 'PERSON', '2024-01-01')"    // Wrong NER type
            );
        }
    }

    @BeforeEach
    void setUpIndexer() throws Exception {
        // Create test stopwords file
        createStopwordsFile();
        
        // Initialize indexer
        String levelDbPath = tempDir.resolve("leveldb").toString();
        indexer = new NerDateIndexGenerator(levelDbPath, TEST_STOPWORDS_PATH, 100, sqliteConn);
    }

    private void createStopwordsFile() throws IOException {
        try (PrintWriter writer = new PrintWriter(TEST_STOPWORDS_PATH)) {
            writer.println("the");
            writer.println("a");
            writer.println("is");
        }
    }

    @AfterEach
    void tearDownIndexer() throws Exception {
        if (indexer != null) {
            indexer.close();
        }
        // Delete test files
        new File(TEST_STOPWORDS_PATH).delete();
    }

    @Test
    void testValidDateIndexing() throws Exception {
        // Generate index
        indexer.generateIndex();
        
        // Check for valid dates
        assertTrue(indexer.hasKey("20240120")); // First date
        assertTrue(indexer.hasKey("20240201")); // Second date
        assertTrue(indexer.hasKey("20240115")); // Third date
        
        // Verify invalid entries are not indexed
        assertFalse(indexer.hasKey("null"));
        assertFalse(indexer.hasKey("invalid-date"));
        assertFalse(indexer.hasKey("20240101")); // Wrong NER type
    }

    @Test
    void testFetchBatch() throws Exception {
        List<AnnotationEntry> batch = indexer.fetchBatch(0);
        
        // Should only get entries where ner = 'DATE' and normalized_ner is not null
        assertEquals(3, batch.size());
        
        // Verify first entry
        AnnotationEntry first = batch.get(0);
        assertEquals(1, first.getDocumentId());
        assertEquals(1, first.getSentenceId());
        assertEquals(0, first.getBeginChar());
        assertEquals(10, first.getEndChar());
        assertEquals("2024-01-20", first.getLemma());
        assertEquals("DATE", first.getPos());
        assertEquals(LocalDate.parse("2024-01-15"), first.getTimestamp());
    }

    @Test
    void testProcessPartition() throws Exception {
        // Create test entries
        List<AnnotationEntry> batch = indexer.fetchBatch(0);
        var result = indexer.processPartition(batch);
        
        // Should have three unique dates
        assertEquals(3, result.keySet().size());
        
        // Verify positions are correctly stored
        assertTrue(result.containsKey("20240120"));
        assertTrue(result.containsKey("20240201"));
        assertTrue(result.containsKey("20240115"));
        
        // Check position details for first date
        List<PositionList> positions = result.get("20240120");
        assertEquals(1, positions.size());
        Position pos = positions.get(0).getPositions().get(0);
        assertEquals(1, pos.getDocumentId());
        assertEquals(1, pos.getSentenceId());
        assertEquals(0, pos.getBeginPosition());
        assertEquals(10, pos.getEndPosition());
    }

    @Test
    void testBasicDateIndexing() throws Exception {
        // Generate index
        indexer.generateIndex();
        
        // Close the indexer to release the file lock
        indexer.close();
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(new File(indexer.getIndexPath()), options)) {
            // Check that valid dates are indexed
            String[] expectedDates = {"20240120", "20240201", "20240115"};
            for (String date : expectedDates) {
                byte[] value = db.get(bytes(date));
                assertNotNull(value, "Date " + date + " should be indexed");
                
                PositionList positions = PositionList.deserialize(value);
                assertTrue(positions.size() > 0, "Should have positions for " + date);
            }
            
            // Verify invalid dates are not indexed
            String[] invalidDates = {"XXXXXXXX", "2024XXXX"};
            for (String date : invalidDates) {
                assertNull(db.get(bytes(date)), "Invalid date " + date + " should not be indexed");
            }
        }
    }
    
    @Test
    void testDateNormalization() throws Exception {
        // Insert dates in different formats
        try (PreparedStatement stmt = sqliteConn.prepareStatement(
            "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) VALUES (?, ?, ?, ?, ?, ?)"
        )) {
            Object[][] data = {
                {1, 4, 0, 10, "DATE", "2024-01-01"},
                {1, 4, 15, 25, "DATE", "2024-01-01"},
                {1, 4, 30, 40, "DATE", "2024-01-01"}
            };
            
            for (Object[] row : data) {
                for (int i = 0; i < row.length; i++) {
                    stmt.setObject(i + 1, row[i]);
                }
                stmt.executeUpdate();
            }
        }
        
        // Generate index
        indexer.generateIndex();
        
        // Close the indexer to release the file lock
        indexer.close();
        
        // Verify date normalization
        Options options = new Options();
        try (DB db = factory.open(new File(indexer.getIndexPath()), options)) {
            // All three date formats should map to the same normalized form
            byte[] value = db.get(bytes("20240101"));
            assertNotNull(value, "Normalized date should be indexed");
            
            PositionList positions = PositionList.deserialize(value);
            assertEquals(3, positions.size(), "Should have three positions for the same normalized date");
        }
    }
    
    @Test
    void testInvalidDateHandling() throws Exception {
        // Insert invalid date formats
        try (PreparedStatement stmt = sqliteConn.prepareStatement(
            "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) VALUES (?, ?, ?, ?, ?, ?)"
        )) {
            Object[][] data = {
                {1, 5, 0, 10, "DATE", "not-a-date"},
                {1, 5, 15, 25, "DATE", "2024-13-01"},  // Invalid month
                {1, 5, 30, 40, "DATE", "2024-01-32"}   // Invalid day
            };
            
            for (Object[] row : data) {
                for (int i = 0; i < row.length; i++) {
                    stmt.setObject(i + 1, row[i]);
                }
                stmt.executeUpdate();
            }
        }
        
        // Generate index
        indexer.generateIndex();
        
        // Close the indexer to release the file lock
        indexer.close();
        
        // Verify invalid dates are not indexed
        Options options = new Options();
        try (DB db = factory.open(new File(indexer.getIndexPath()), options)) {
            assertNull(db.get(bytes("not-a-date")), "Invalid date format should not be indexed");
            assertNull(db.get(bytes("20241301")), "Invalid month should not be indexed");
            assertNull(db.get(bytes("20240132")), "Invalid day should not be indexed");
        }
    }
} 
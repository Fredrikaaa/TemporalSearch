package com.example.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NerDateIndexGeneratorTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-ner.txt";
    private Connection conn;
    private File dbFile;
    private NerDateIndexGenerator indexer;
    
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        // Create test stopwords file
        createStopwordsFile();

        // Create temporary SQLite database
        dbFile = tempDir.resolve("test.db").toFile();
        conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
        
        // Create test tables
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE documents (" +
                "document_id INTEGER PRIMARY KEY, " +
                "timestamp TEXT NOT NULL)"
            );
            stmt.execute(
                "CREATE TABLE annotations (" +
                "document_id INTEGER, " +
                "sentence_id INTEGER, " +
                "begin_char INTEGER, " +
                "end_char INTEGER, " +
                "ner TEXT, " +
                "normalized_ner TEXT)"
            );
            
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
        
        // Initialize indexer
        String levelDbPath = tempDir.resolve("leveldb").toString();
        indexer = new NerDateIndexGenerator(levelDbPath, TEST_STOPWORDS_PATH, 100, conn);
    }

    private void createStopwordsFile() throws IOException {
        try (PrintWriter writer = new PrintWriter(TEST_STOPWORDS_PATH)) {
            writer.println("the");
            writer.println("a");
            writer.println("is");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexer != null) {
            indexer.close();
        }
        if (conn != null) {
            conn.close();
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
} 
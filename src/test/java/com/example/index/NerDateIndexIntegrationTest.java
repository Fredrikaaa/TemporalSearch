package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;

class NerDateIndexIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(NerDateIndexIntegrationTest.class);
    private Path tempDir;
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories and files
        tempDir = Files.createTempDirectory("ner-date-test-");
        levelDbPath = tempDir.resolve("test-index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        sqlitePath = tempDir.resolve("test.db");
        
        // Create empty stopwords file (not needed for date indexing)
        Files.write(stopwordsPath, new ArrayList<String>());
        
        // Create and populate SQLite database
        sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        setupDatabase();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (sqliteConn != null && !sqliteConn.isClosed()) {
            sqliteConn.close();
        }
        // Clean up temporary files
        Files.walk(tempDir)
            .sorted(Comparator.reverseOrder())
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    // Ignore cleanup errors
                }
            });
    }
    
    private void setupDatabase() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Create documents table
            stmt.execute("""
                CREATE TABLE documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL
                )
            """);
            
            // Create annotations table
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
            stmt.execute("""
                INSERT INTO documents (document_id, timestamp) VALUES
                (1, '2024-01-15T10:00:00Z'),
                (2, '2024-01-16T10:00:00Z'),
                (3, '2024-01-17T10:00:00Z')
            """);
            
            // Insert test data with various date formats and edge cases
            stmt.execute("""
                INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) VALUES
                (1, 1, 0, 10, 'DATE', '2024-01-15'),
                (1, 2, 15, 25, 'DATE', '2023-12-31'),
                (2, 1, 5, 15, 'DATE', '2024-02-29'),
                (2, 2, 20, 30, 'DATE', null),
                (2, 3, 35, 45, 'DATE', 'invalid-date'),
                (3, 1, 0, 10, 'PERSON', '2024-01-16'),
                (3, 2, 15, 25, 'DATE', '2024-01-16')
            """);
        }
    }
    
    @Test
    void testEndToEndDateIndexing() throws Exception {
        // Create and run the indexer
        try (NerDateIndexGenerator indexer = new NerDateIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(), 10, sqliteConn)) {
            indexer.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(levelDbPath.toFile(), options)) {
            // Check valid dates are indexed
            assertNotNull(db.get(bytes("20240115")), "2024-01-15 should be indexed");
            assertNotNull(db.get(bytes("20231231")), "2023-12-31 should be indexed");
            assertNotNull(db.get(bytes("20240229")), "2024-02-29 should be indexed");
            assertNotNull(db.get(bytes("20240116")), "2024-01-16 should be indexed");
            
            // Verify specific positions for a date
            PositionList positions = PositionList.deserialize(db.get(bytes("20240116")));
            assertEquals(1, positions.size(), "Should only index DATE entities");
            
            Position pos = positions.getPositions().get(0);
            assertEquals(3, pos.getDocumentId());
            assertEquals(2, pos.getSentenceId());
            assertEquals(15, pos.getBeginPosition());
            assertEquals(25, pos.getEndPosition());
            
            // Verify invalid dates are not indexed
            assertNull(db.get(bytes("invalid-date")), "Invalid date should not be indexed");
            assertNull(db.get(bytes("null")), "Null date should not be indexed");
        }
    }
    
    @Test
    void testLargeDataset() throws Exception {
        // Insert a larger dataset
        try (Statement stmt = sqliteConn.createStatement()) {
            // Generate 1000 test dates across 100 documents
            for (int doc = 1; doc <= 100; doc++) {
                // Insert document if it doesn't exist
                stmt.execute(String.format(
                    "INSERT OR IGNORE INTO documents (document_id, timestamp) VALUES (%d, '2024-01-%02dT10:00:00Z')",
                    doc + 3, // Start after existing docs
                    Math.min(doc, 31) // Keep days valid
                ));
                
                // Ensure we generate the specific dates we want to test
                if (doc == 1) {
                    logger.info("Inserting test date: 2024-01-02");
                    stmt.execute(
                        "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) " +
                        "VALUES (4, 1, 0, 10, 'DATE', '2024-01-02')"
                    );
                    logger.info("Inserting test date: 2024-02-15");
                    stmt.execute(
                        "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) " +
                        "VALUES (4, 2, 0, 10, 'DATE', '2024-02-15')"
                    );
                    logger.info("Inserting test date: 2024-04-01");
                    stmt.execute(
                        "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) " +
                        "VALUES (4, 3, 0, 10, 'DATE', '2024-04-01')"
                    );
                }
                
                // Generate additional random dates
                for (int sentence = 1; sentence <= 10; sentence++) {
                    // Generate dates from January to April 2024
                    int dayOffset = doc + sentence;
                    int month = (dayOffset / 31) + 1; // Increment month every 31 days
                    int day = (dayOffset % 31) + 1;   // Keep days between 1-31
                    if (month > 4) month = 4;         // Cap at April
                    if (day > 28 && month == 2) day = 28; // Handle February
                    
                    LocalDate date = LocalDate.of(2024, month, day);
                    String normalizedDate = date.toString();
                    stmt.execute(String.format(
                        "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, ner, normalized_ner) " +
                        "VALUES (%d, %d, 0, 10, 'DATE', '%s')",
                        doc + 3, sentence + 3, normalizedDate // Start at sentence 4 to avoid conflicts
                    ));
                }
            }
        }
        
        // Create and run the indexer
        long startTime = System.currentTimeMillis();
        try (NerDateIndexGenerator indexer = new NerDateIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(), 50, sqliteConn)) {
            indexer.generateIndex();
        }
        long duration = System.currentTimeMillis() - startTime;
        
        // Verify performance and correctness
        assertTrue(duration < 30000, "Large dataset should process in under 30 seconds");
        
        Options options = new Options();
        try (DB db = factory.open(levelDbPath.toFile(), options)) {
            // Check a few random dates that we know should exist
            assertNotNull(db.get(bytes("20240102")), "2024-01-02 should be indexed");
            assertNotNull(db.get(bytes("20240215")), "2024-02-15 should be indexed");
            assertNotNull(db.get(bytes("20240401")), "2024-04-01 should be indexed");
            
            // Verify position counts
            PositionList positions = PositionList.deserialize(db.get(bytes("20240102")));
            assertNotNull(positions, "Should have positions for date");
            assertTrue(positions.size() > 0, "Should have at least one position");
            
            // Verify some edge cases
            assertNull(db.get(bytes("20240230")), "2024-02-30 should not be indexed (invalid date)");
            assertNull(db.get(bytes("20240431")), "2024-04-31 should not be indexed (invalid date)");
        }
    }
} 
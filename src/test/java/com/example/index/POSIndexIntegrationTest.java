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
import java.util.*;

class POSIndexIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(POSIndexIntegrationTest.class);
    private Path tempDir;
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories and files
        tempDir = Files.createTempDirectory("pos-test-");
        levelDbPath = tempDir.resolve("test-index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        sqlitePath = tempDir.resolve("test.db");
        
        // Create empty stopwords file (not used for POS indexing)
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
                    logger.error("Failed to delete {}", path, e);
                }
            });
    }
    
    private void setupDatabase() throws SQLException {
        // Drop existing tables if they exist
        sqliteConn.createStatement().execute("DROP TABLE IF EXISTS annotations");
        sqliteConn.createStatement().execute("DROP TABLE IF EXISTS documents");

        // Create documents table
        sqliteConn.createStatement().execute(
            "CREATE TABLE documents (" +
            "document_id INTEGER PRIMARY KEY, " +
            "timestamp TEXT NOT NULL" +
            ")"
        );

        // Insert test documents
        sqliteConn.createStatement().execute(
            "INSERT INTO documents (document_id, timestamp) VALUES " +
            "(1, '2024-01-01T10:00:00Z'), " +
            "(2, '2024-01-02T10:00:00Z')"
        );
        
        // Create annotations table with correct column names
        sqliteConn.createStatement().execute("""
            CREATE TABLE annotations (
                document_id INTEGER,
                sentence_id INTEGER,
                begin_char INTEGER,
                end_char INTEGER,
                lemma TEXT,
                pos TEXT,
                FOREIGN KEY(document_id) REFERENCES documents(document_id)
            )
        """);
        
        // Insert test annotations
        sqliteConn.createStatement().execute("""
            INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos)
            VALUES 
                (1, 1, 0, 3, 'the', 'DT'),
                (1, 1, 4, 9, 'quick', 'JJ'),
                (1, 1, 10, 15, 'brown', 'JJ'),
                (1, 1, 16, 19, 'fox', 'NN'),
                (1, 1, 20, 26, 'jumped', 'VBD'),
                (1, 1, 27, 31, 'over', 'IN'),
                (1, 1, 32, 35, 'the', 'DT'),
                (1, 1, 36, 40, 'lazy', 'JJ'),
                (1, 1, 41, 44, 'dog', 'NN')
        """);
    }
    
    @Test
    void testBasicPOSIndexing() throws SQLException, IOException {
        setupDatabase();
        
        try (POSIndexGenerator indexer = new POSIndexGenerator(
            levelDbPath.toString(),
            stopwordsPath.toString(),
            10, // Small batch size to test batching
            sqliteConn
        )) {
            indexer.generateIndex();
            
            // Check adjectives (JJ)
            byte[] posListBytes = indexer.levelDb.get("jj".getBytes());
            assertNotNull(posListBytes, "Should have adjectives");
            PositionList posList = PositionList.deserialize(posListBytes);
            assertEquals(3, posList.size(), "Should have three adjectives (quick, brown, lazy)");
            
            // Check nouns (NN)
            posListBytes = indexer.levelDb.get("nn".getBytes());
            assertNotNull(posListBytes, "Should have nouns");
            posList = PositionList.deserialize(posListBytes);
            assertEquals(2, posList.size(), "Should have two nouns (fox, dog)");
        }
    }
    
    @Test
    void testLargeDataset() throws SQLException, IOException {
        // Create a new database for this test to avoid document ID conflicts
        try (Statement stmt = sqliteConn.createStatement()) {
            // Drop existing tables if they exist
            stmt.execute("DROP TABLE IF EXISTS annotations");
            stmt.execute("DROP TABLE IF EXISTS documents");
            
            // Create tables
            stmt.execute("CREATE TABLE documents (document_id INTEGER PRIMARY KEY, timestamp TEXT)");
            stmt.execute("""
                CREATE TABLE annotations (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    begin_char INTEGER,
                    end_char INTEGER,
                    lemma TEXT,
                    pos TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert 100 documents with timestamps
            for (int i = 1; i <= 100; i++) {
                int day = (i % 28) + 1; // Ensure days are between 1-28
                stmt.execute(String.format(
                    "INSERT INTO documents (document_id, timestamp) VALUES (%d, '2024-01-%02dT10:00:00Z')",
                    i, day
                ));
                
                // Insert POS annotations for each document
                stmt.execute(String.format("""
                    INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos)
                    VALUES 
                        (%d, 1, 0, 3, 'The', 'DT'),
                        (%d, 1, 4, 8, 'test', 'NN'),
                        (%d, 1, 9, 14, 'works', 'VBZ'),
                        (%d, 1, 15, 19, 'well', 'RB')
                """, i, i, i, i));
            }
        }
        
        // Create and run the indexer
        try (POSIndexGenerator indexer = new POSIndexGenerator(
            levelDbPath.toString(),
            stopwordsPath.toString(),
            10, // Small batch size to test batching
            sqliteConn
        )) {
            indexer.generateIndex();
            
            // Verify index contents
            byte[] posListBytes = indexer.levelDb.get("nn".getBytes());
            assertNotNull(posListBytes, "Should have noun entries");
            
            PositionList posList = PositionList.deserialize(posListBytes);
            assertEquals(100, posList.size(), "Should have 100 noun entries (one per document)");
        }
    }
} 
package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;

class DependencyIndexGeneratorTest {
    private Path tempDir;
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories and files
        tempDir = Files.createTempDirectory("dep-index-test-");
        levelDbPath = tempDir.resolve("test-index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        sqlitePath = tempDir.resolve("test.db");
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an");
        Files.write(stopwordsPath, stopwords);
        
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
                     Files.deleteIfExists(path);
                 } catch (IOException e) {
                     // Ignore cleanup errors
                 }
             });
    }
    
    private void setupDatabase() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Create tables
            stmt.execute("""
                CREATE TABLE documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL
                )
            """);
            
            stmt.execute("""
                CREATE TABLE sentences (
                    id INTEGER PRIMARY KEY,
                    doc_id INTEGER,
                    text TEXT,
                    timestamp INTEGER,
                    FOREIGN KEY(doc_id) REFERENCES documents(document_id)
                )
            """);
            
            stmt.execute("""
                CREATE TABLE dependencies (
                    sentence_id INTEGER,
                    document_id INTEGER,
                    head_token TEXT,
                    dependent_token TEXT,
                    relation TEXT,
                    begin_char INTEGER,
                    end_char INTEGER,
                    FOREIGN KEY(sentence_id) REFERENCES sentences(id),
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20T10:00:00Z')");
            stmt.execute("INSERT INTO sentences (id, doc_id, text, timestamp) VALUES (1, 1, 'cat chases mouse', 1000)");
            
            // Insert test dependencies for "cat chases mouse"
            // cat -> chases (nsubj)
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES (1, 1, 'chases', 'cat', 'nsubj', 0, 3)
            """);
            
            // chases -> mouse (dobj)
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES (1, 1, 'chases', 'mouse', 'dobj', 11, 16)
            """);
        }
    }
    
    @Test
    void testBasicDependencyIndexing() throws Exception {
        // Test with different thread counts
        int[] threadCounts = {1, 2};
        
        for (int threadCount : threadCounts) {
            Path indexPath = tempDir.resolve("dep-" + threadCount);
            
            try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                    indexPath.toString(), stopwordsPath.toString(), 
                    10, sqliteConn, threadCount)) {
                generator.generateIndex();
            }
            
            // Verify index contents
            Options options = new Options();
            try (DB db = factory.open(indexPath.toFile(), options)) {
                // Check expected dependencies
                String[] expectedKeys = {
                    "chases\0nsubj\0cat",   // Subject dependency
                    "chases\0dobj\0mouse"    // Object dependency
                };
                
                for (String key : expectedKeys) {
                    assertNotNull(db.get(bytes(key)), "Key should exist: " + key);
                    
                    // Verify position information
                    PositionList positions = PositionList.deserialize(db.get(bytes(key)));
                    assertEquals(1, positions.size(), "Should have one position for " + key);
                    
                    Position pos = positions.getPositions().get(0);
                    assertEquals(1, pos.getDocumentId());
                    assertEquals(1, pos.getSentenceId());
                    assertTrue(pos.getBeginPosition() >= 0);
                    assertTrue(pos.getEndPosition() <= 16); // "cat chases mouse".length() = 16
                }
            }
        }
    }
} 
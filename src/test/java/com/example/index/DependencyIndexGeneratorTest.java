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
                CREATE TABLE annotations (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    begin_char INTEGER,
                    end_char INTEGER,
                    lemma TEXT,
                    pos TEXT,
                    FOREIGN KEY (document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20T10:00:00Z')");
            
            // Insert test annotations - sentence: "cat chases mouse"
            String[][] words = {
                {"cat", "NOUN"}, {"chases", "VERB"}, {"mouse", "NOUN"}
            };
            
            int charPos = 0;
            for (int i = 0; i < words.length; i++) {
                stmt.execute(String.format(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) " +
                    "VALUES (1, 1, %d, %d, '%s', '%s')",
                    charPos, charPos + words[i][0].length(), words[i][0], words[i][1]
                ));
                charPos += words[i][0].length() + 1; // +1 for space
            }
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
                // Check some expected dependencies
                String[] expectedKeys = {
                    "cat\u0000dep\u0000chases",
                    "cat\u0000dep\u0000mouse",
                    "chases\u0000dep\u0000cat",
                    "chases\u0000dep\u0000mouse",
                    "mouse\u0000dep\u0000cat",
                    "mouse\u0000dep\u0000chases"
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
                    assertTrue(pos.getEndPosition() <= 16); // "cat chases mouse".length() = 3 + 1 + 6 + 1 + 5 = 16
                }
            }
        }
    }
} 
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

class IndexGeneratorTest {
    private Path tempDir;
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories and files
        tempDir = Files.createTempDirectory("index-test-");
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
        if (sqliteConn != null) {
            sqliteConn.close();
        }
        
        // Clean up temporary files
        Files.walk(tempDir)
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try {
                     Files.delete(path);
                 } catch (IOException e) {
                     e.printStackTrace();
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
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20T10:00:00Z')");
            
            // Insert test annotations - sentence: "quick brown fox jumps over lazy dog"
            String[][] words = {
                {"quick", "ADJ"}, {"brown", "ADJ"}, {"fox", "NOUN"},
                {"jumps", "VERB"}, {"over", "ADP"}, {"lazy", "ADJ"}, {"dog", "NOUN"}
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
    void testUnigramIndexGeneration() throws Exception {
        // Test with different thread counts
        int[] threadCounts = {1, 2, 4};
        
        for (int threadCount : threadCounts) {
            Path indexPath = tempDir.resolve("unigram-" + threadCount);
            
            try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                    indexPath.toString(), stopwordsPath.toString(), 
                    10, sqliteConn, threadCount)) {
                generator.generateIndex();
            }
            
            // Verify index contents
            Options options = new Options();
            try (DB db = factory.open(indexPath.toFile(), options)) {
                // Check some expected unigrams
                assertNotNull(db.get(bytes("quick")), "quick should be indexed");
                assertNotNull(db.get(bytes("fox")), "fox should be indexed");
                assertNotNull(db.get(bytes("dog")), "dog should be indexed");
                
                // Verify a specific position
                PositionList quickPositions = PositionList.deserialize(db.get(bytes("quick")));
                assertEquals(1, quickPositions.size(), "quick should appear once");
                
                Position pos = quickPositions.getPositions().get(0);
                assertEquals(1, pos.getDocumentId());
                assertEquals(1, pos.getSentenceId());
                assertEquals(0, pos.getBeginPosition());
                assertEquals(5, pos.getEndPosition());
            }
        }
    }
    
    @Test
    void testBigramIndexGeneration() throws Exception {
        Path indexPath = tempDir.resolve("bigram");
        
        try (BigramIndexGenerator generator = new BigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn, 2)) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check some expected bigrams
            assertNotNull(db.get(bytes("quick\u0000brown")), "quick brown should be indexed");
            assertNotNull(db.get(bytes("brown\u0000fox")), "brown fox should be indexed");
            
            // Verify a specific position
            PositionList quickBrownPositions = PositionList.deserialize(db.get(bytes("quick\u0000brown")));
            assertEquals(1, quickBrownPositions.size(), "quick brown should appear once");
            
            Position pos = quickBrownPositions.getPositions().get(0);
            assertEquals(1, pos.getDocumentId());
            assertEquals(1, pos.getSentenceId());
            assertEquals(0, pos.getBeginPosition());
            assertTrue(pos.getEndPosition() > pos.getBeginPosition(), "end char should be after begin char");
        }
    }
    
    @Test
    void testTrigramIndexGeneration() throws Exception {
        Path indexPath = tempDir.resolve("trigram");
        
        try (TrigramIndexGenerator generator = new TrigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn, 2)) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check some expected trigrams
            assertNotNull(db.get(bytes("quick\u0000brown\u0000fox")), "quick brown fox should be indexed");
            assertNotNull(db.get(bytes("brown\u0000fox\u0000jumps")), "brown fox jumps should be indexed");
            
            // Verify a specific position
            PositionList quickBrownFoxPositions = PositionList.deserialize(db.get(bytes("quick\u0000brown\u0000fox")));
            assertEquals(1, quickBrownFoxPositions.size(), "quick brown fox should appear once");
            
            Position pos = quickBrownFoxPositions.getPositions().get(0);
            assertEquals(1, pos.getDocumentId());
            assertEquals(1, pos.getSentenceId());
            assertEquals(0, pos.getBeginPosition());
            assertTrue(pos.getEndPosition() > pos.getBeginPosition(), "end char should be after begin char");
        }
    }
    
    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
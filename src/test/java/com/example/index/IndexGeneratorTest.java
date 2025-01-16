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
import java.util.stream.Collectors;

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
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (2, '2024-01-21T10:00:00Z')");
            
            // Insert test annotations with repeated words and phrases
            // Document 1: "quick brown fox jumps over lazy dog"
            // Document 2: "quick brown fox runs past quick brown rabbit"
            String[][] words = {
                // Document 1
                {"quick", "ADJ", "1", "1", "0", "5"},
                {"brown", "ADJ", "1", "1", "6", "11"},
                {"fox", "NOUN", "1", "1", "12", "15"},
                {"jumps", "VERB", "1", "1", "16", "21"},
                {"over", "ADP", "1", "1", "22", "26"},
                {"lazy", "ADJ", "1", "1", "27", "31"},
                {"dog", "NOUN", "1", "1", "32", "35"},
                // Document 2
                {"quick", "ADJ", "2", "1", "0", "5"},
                {"brown", "ADJ", "2", "1", "6", "11"},
                {"fox", "NOUN", "2", "1", "12", "15"},
                {"runs", "VERB", "2", "1", "16", "20"},
                {"past", "ADP", "2", "1", "21", "25"},
                {"quick", "ADJ", "2", "1", "26", "31"},
                {"brown", "ADJ", "2", "1", "32", "37"},
                {"rabbit", "NOUN", "2", "1", "38", "44"}
            };
            
            for (String[] word : words) {
                stmt.execute(String.format(
                    "INSERT INTO annotations (lemma, pos, document_id, sentence_id, begin_char, end_char) " +
                    "VALUES ('%s', '%s', %s, %s, %s, %s)",
                    word[0], word[1], word[2], word[3], word[4], word[5]
                ));
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
                assertEquals(3, quickPositions.size(), "quick should appear three times");
                
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
            assertEquals(3, quickBrownPositions.size(), "quick brown should appear three times");
            
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
            assertEquals(2, quickBrownFoxPositions.size(), "quick brown fox should appear twice");
            
            Position pos = quickBrownFoxPositions.getPositions().get(0);
            assertEquals(1, pos.getDocumentId());
            assertEquals(1, pos.getSentenceId());
            assertEquals(0, pos.getBeginPosition());
            assertTrue(pos.getEndPosition() > pos.getBeginPosition(), "end char should be after begin char");
        }
    }
    
    @Test
    void testUnigramAggregation() throws Exception {
        Path indexPath = tempDir.resolve("unigram-aggregation");
        
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check words that appear multiple times
            byte[] quickData = db.get(bytes("quick"));
            assertNotNull(quickData, "quick should be indexed");
            PositionList quickPositions = PositionList.deserialize(quickData);
            assertEquals(3, quickPositions.size(), "quick should appear 3 times");

            byte[] brownData = db.get(bytes("brown"));
            assertNotNull(brownData, "brown should be indexed");
            PositionList brownPositions = PositionList.deserialize(brownData);
            assertEquals(3, brownPositions.size(), "brown should appear 3 times");

            // Check words that appear once
            byte[] dogData = db.get(bytes("dog"));
            assertNotNull(dogData, "dog should be indexed");
            PositionList dogPositions = PositionList.deserialize(dogData);
            assertEquals(1, dogPositions.size(), "dog should appear once");
        }
    }
    
    @Test
    void testBigramAggregation() throws Exception {
        Path indexPath = tempDir.resolve("bigram-aggregation");
        
        try (BigramIndexGenerator generator = new BigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check bigram that appears multiple times
            String repeatedBigram = "quick" + BaseIndexGenerator.DELIMITER + "brown";
            byte[] repeatedData = db.get(bytes(repeatedBigram));
            assertNotNull(repeatedData, "quick brown should be indexed");
            PositionList repeatedPositions = PositionList.deserialize(repeatedData);
            assertEquals(3, repeatedPositions.size(), "quick brown should appear 3 times");

            // Verify positions are from different locations
            Set<String> uniquePositions = repeatedPositions.getPositions().stream()
                .map(p -> String.format("%d-%d", p.getDocumentId(), p.getSentenceId()))
                .collect(Collectors.toSet());
            assertTrue(uniquePositions.size() > 1, "Positions should be from different locations");

            // Check bigram that appears once
            String singleBigram = "lazy" + BaseIndexGenerator.DELIMITER + "dog";
            byte[] singleData = db.get(bytes(singleBigram));
            assertNotNull(singleData, "lazy dog should be indexed");
            PositionList singlePositions = PositionList.deserialize(singleData);
            assertEquals(1, singlePositions.size(), "lazy dog should appear once");
        }
    }
    
    @Test
    void testTrigramAggregation() throws Exception {
        Path indexPath = tempDir.resolve("trigram-aggregation");
        
        try (TrigramIndexGenerator generator = new TrigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check trigram that appears multiple times
            String repeatedTrigram = "quick" + BaseIndexGenerator.DELIMITER + "brown" + BaseIndexGenerator.DELIMITER + "fox";
            byte[] repeatedData = db.get(bytes(repeatedTrigram));
            assertNotNull(repeatedData, "quick brown fox should be indexed");
            PositionList repeatedPositions = PositionList.deserialize(repeatedData);
            assertEquals(2, repeatedPositions.size(), "quick brown fox should appear twice");

            // Verify positions are from different documents
            Set<Integer> documents = repeatedPositions.getPositions().stream()
                .map(Position::getDocumentId)
                .collect(Collectors.toSet());
            assertEquals(2, documents.size(), "Should have positions from both documents");

            // Check trigram that appears once
            String singleTrigram = "brown" + BaseIndexGenerator.DELIMITER + "fox" + BaseIndexGenerator.DELIMITER + "jumps";
            byte[] singleData = db.get(bytes(singleTrigram));
            assertNotNull(singleData, "brown fox jumps should be indexed");
            PositionList singlePositions = PositionList.deserialize(singleData);
            assertEquals(1, singlePositions.size(), "brown fox jumps should appear once");
        }
    }
    
    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
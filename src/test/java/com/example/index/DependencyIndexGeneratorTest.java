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

class DependencyIndexGeneratorTest {
    private Path tempDir;
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    private static final String TEST_DB_PATH = "test-leveldb-dependency";

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
                CREATE TABLE dependencies (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    head_token TEXT,
                    dependent_token TEXT,
                    relation TEXT,
                    begin_char INTEGER,
                    end_char INTEGER,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20 10:00:00.000')");
            
            // Insert test dependencies
            // "The cat chases the mouse"
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token, dependent_token, relation, begin_char, end_char)
                VALUES 
                    (1, 1, 'chases', 'cat', 'nsubj', 4, 7),
                    (1, 1, 'cat', 'The', 'det', 0, 3),
                    (1, 1, 'chases', 'mouse', 'dobj', 11, 16)
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
                    "chases" + BaseIndexGenerator.DELIMITER + "nsubj" + BaseIndexGenerator.DELIMITER + "cat",   // Subject dependency
                    "chases" + BaseIndexGenerator.DELIMITER + "dobj" + BaseIndexGenerator.DELIMITER + "mouse"    // Object dependency
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

    @Test
    void testDependencyAggregation() throws Exception {
        // Setup test data with repeated dependencies
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (2, '2024-01-21 10:00:00.000')");
            
            // Add more instances of the same dependency pattern (cat-nsubj->chases)
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES 
                    (2, 1, 'chases', 'cat', 'nsubj', 0, 3),
                    (2, 2, 'chases', 'cat', 'nsubj', 20, 23)
            """);
        }

        // Generate index
        Path indexPath = tempDir.resolve("dep-aggregation");
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }

        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check the aggregated dependency
            String key = "chases" + BaseIndexGenerator.DELIMITER + "nsubj" + BaseIndexGenerator.DELIMITER + "cat";
            byte[] data = db.get(bytes(key));
            assertNotNull(data, "Dependency should exist: " + key);

            PositionList positions = PositionList.deserialize(data);
            assertEquals(3, positions.size(), "Should have 3 occurrences of cat-nsubj->chases");

            // Verify positions are from different documents/sentences
            Set<String> uniquePositions = positions.getPositions().stream()
                .map(p -> String.format("%d-%d", p.getDocumentId(), p.getSentenceId()))
                .collect(Collectors.toSet());
            assertEquals(3, uniquePositions.size(), "Should have positions from 3 different locations");
        }
    }

    @Test
    void testCaseNormalization() throws IOException {
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
                
            DependencyEntry entry = new DependencyEntry(
                1, 1, 0, 10, "April", "Day", "compound", LocalDate.now());
                
            String key = generator.generateKey(entry);
            assertEquals("april" + BaseIndexGenerator.DELIMITER + "compound" + 
                        BaseIndexGenerator.DELIMITER + "day", key);
        }
    }

    @Test
    void testBlacklistedRelations() throws Exception {
        // Setup test data with blacklisted relations
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (3, '2024-01-22 10:00:00.000')");
            
            // Add dependencies with blacklisted relations
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES 
                    (3, 1, 'cat', 'the', 'det', 0, 3),
                    (3, 1, 'mouse', 'the', 'det', 8, 11),
                    (3, 1, 'chases', 'and', 'cc', 15, 18),
                    (3, 1, 'mouse', 'with', 'case', 20, 24)
            """);
        }

        // Generate index
        Path indexPath = tempDir.resolve("blacklist-test");
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }

        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check that blacklisted relations are not indexed
            String[] blacklistedKeys = {
                "cat" + BaseIndexGenerator.DELIMITER + "det" + BaseIndexGenerator.DELIMITER + "the",
                "mouse" + BaseIndexGenerator.DELIMITER + "det" + BaseIndexGenerator.DELIMITER + "the",
                "chases" + BaseIndexGenerator.DELIMITER + "cc" + BaseIndexGenerator.DELIMITER + "and",
                "mouse" + BaseIndexGenerator.DELIMITER + "case" + BaseIndexGenerator.DELIMITER + "with"
            };
            
            for (String key : blacklistedKeys) {
                assertNull(db.get(bytes(key)), 
                    "Blacklisted relation should not be indexed: " + key);
            }
        }
    }

    @Test
    void testStopwordFiltering() throws Exception {
        // Setup test data with stopwords
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (4, '2024-01-23 10:00:00.000')");
            
            // Add dependencies with stopwords
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES 
                    (4, 1, 'the', 'cat', 'nsubj', 0, 3),
                    (4, 1, 'cat', 'the', 'det', 4, 7),
                    (4, 1, 'a', 'mouse', 'det', 8, 9)
            """);
        }

        // Generate index
        Path indexPath = tempDir.resolve("stopword-test");
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }

        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check that entries with stopwords are not indexed
            String[] stopwordKeys = {
                "the" + BaseIndexGenerator.DELIMITER + "nsubj" + BaseIndexGenerator.DELIMITER + "cat",
                "cat" + BaseIndexGenerator.DELIMITER + "det" + BaseIndexGenerator.DELIMITER + "the",
                "a" + BaseIndexGenerator.DELIMITER + "det" + BaseIndexGenerator.DELIMITER + "mouse"
            };
            
            for (String key : stopwordKeys) {
                assertNull(db.get(bytes(key)), 
                    "Entry with stopword should not be indexed: " + key);
            }
        }
    }

    @Test
    void testNullAndInvalidEntries() throws Exception {
        // Setup test data with null values
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (5, '2024-01-24 10:00:00.000')");
            
            // Add dependencies with null values
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES 
                    (5, 1, NULL, 'cat', 'nsubj', 0, 3),
                    (5, 1, 'cat', NULL, 'dobj', 4, 7),
                    (5, 1, 'mouse', 'cat', NULL, 8, 11)
            """);
        }

        // Generate index
        Path indexPath = tempDir.resolve("null-test");
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            generator.generateIndex();
        }

        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check that entries with null values are not indexed
            String[] nullKeys = {
                "null" + BaseIndexGenerator.DELIMITER + "nsubj" + BaseIndexGenerator.DELIMITER + "cat",
                "cat" + BaseIndexGenerator.DELIMITER + "dobj" + BaseIndexGenerator.DELIMITER + "null",
                "mouse" + BaseIndexGenerator.DELIMITER + "null" + BaseIndexGenerator.DELIMITER + "cat"
            };
            
            for (String key : nullKeys) {
                assertNull(db.get(bytes(key)), 
                    "Entry with null values should not be indexed: " + key);
            }
        }
    }

    @Test
    void testKeyGenerationConsistency() throws IOException {
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn)) {
            
            // Test various input formats
            DependencyEntry[] entries = {
                new DependencyEntry(1, 1, 0, 10, "Cat", "Mouse", "nsubj", LocalDate.now()),
                new DependencyEntry(1, 1, 0, 10, "CAT", "MOUSE", "NSUBJ", LocalDate.now()),
                new DependencyEntry(1, 1, 0, 10, "cat", "mouse", "nsubj", LocalDate.now()),
                new DependencyEntry(1, 1, 0, 10, " Cat ", " Mouse ", " nsubj ", LocalDate.now())
            };
            
            String expectedKey = "cat" + BaseIndexGenerator.DELIMITER + "nsubj" + 
                               BaseIndexGenerator.DELIMITER + "mouse";
            
            // All entries should generate the same key
            for (DependencyEntry entry : entries) {
                assertEquals(expectedKey, generator.generateKey(entry),
                    "Key generation should be consistent regardless of input format");
            }
        }
    }
} 
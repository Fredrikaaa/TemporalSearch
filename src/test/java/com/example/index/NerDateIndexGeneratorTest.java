package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import com.example.logging.ProgressTracker;

public class NerDateIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-date.txt";
    private File levelDbDir;

    @BeforeEach
    @Override
    void setUp() throws Exception {
        super.setUp();
        
        // Create test stopwords file
        try (PrintWriter writer = new PrintWriter(TEST_STOPWORDS_PATH)) {
            writer.println("the");
            writer.println("a");
            writer.println("is");
        }

        // Set up LevelDB directory
        levelDbDir = tempDir.resolve("test-leveldb-date").toFile();
        if (levelDbDir.exists()) {
            deleteDirectory(levelDbDir);
        }
        levelDbDir.mkdir();

        // Insert test data
        setupTestData();
    }

    private void setupTestData() throws SQLException {
        // Insert documents with timestamps
        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO documents (document_id, timestamp) VALUES (?, ?)")) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, "2024-01-28");
            pstmt.executeUpdate();
            
            pstmt.setInt(1, 2);
            pstmt.setString(2, "2024-01-28");
            pstmt.executeUpdate();
        }

        // Insert test annotations with dates:
        // "The event happened on January 15, 2024"
        // "Another event is scheduled for 2024-02-01"
        String[][] testAnnotations = {
            // Document 1, Sentence 1
            { "1", "0", "20", "32", "January 15, 2024", "2024-01-15", "DATE" },
            // Document 2, Sentence 1
            { "2", "0", "25", "35", "2024-02-01", "2024-02-01", "DATE" }
        };

        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, normalized_ner, ner) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String[] annotation : testAnnotations) {
                pstmt.setInt(1, Integer.parseInt(annotation[0]));
                pstmt.setInt(2, Integer.parseInt(annotation[1]));
                pstmt.setInt(3, Integer.parseInt(annotation[2]));
                pstmt.setInt(4, Integer.parseInt(annotation[3]));
                pstmt.setString(5, annotation[4]);
                pstmt.setString(6, annotation[5]);
                pstmt.setString(7, annotation[6]);
                pstmt.executeUpdate();
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        super.tearDown();
        new File(TEST_STOPWORDS_PATH).delete();
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            for (File file : dir.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
            dir.delete();
        }
    }

    @Test
    public void testBasicDateIndexing() throws Exception {
        // Create and run date indexer
        try (NerDateIndexGenerator indexer = new NerDateIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Test first date
            String key = "20240115";
            PositionList positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for 2024-01-15");
            assertEquals(1, positions.size(), "Should have one position for 2024-01-15");

            // Test second date
            key = "20240201";
            positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for 2024-02-01");
            assertEquals(1, positions.size(), "Should have one position for 2024-02-01");
        }
    }

    @Test
    public void testDateNormalization() throws Exception {
        // Insert document record for additional test data
        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO documents (document_id, timestamp) VALUES (?, ?)")) {
            pstmt.setInt(1, 3);
            pstmt.setString(2, "2024-01-28");
            pstmt.executeUpdate();
        }

        // Insert additional date annotations with different formats
        String[][] additionalDates = {
            { "3", "0", "0", "10", "2024-01-15", "2024-01-15", "DATE" },
            { "3", "0", "11", "21", "2024-01-15", "2024-01-15", "DATE" }
        };

        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, normalized_ner, ner) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String[] annotation : additionalDates) {
                pstmt.setInt(1, Integer.parseInt(annotation[0]));
                pstmt.setInt(2, Integer.parseInt(annotation[1]));
                pstmt.setInt(3, Integer.parseInt(annotation[2]));
                pstmt.setInt(4, Integer.parseInt(annotation[3]));
                pstmt.setString(5, annotation[4]);
                pstmt.setString(6, annotation[5]);
                pstmt.setString(7, annotation[6]);
                pstmt.executeUpdate();
            }
        }

        // Generate index
        try (NerDateIndexGenerator indexer = new NerDateIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Verify date normalization
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            String key = "20240115";
            PositionList positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for 2024-01-15");
            assertEquals(3, positions.size(), "Should have three positions for 2024-01-15 (1 from setup + 2 from additional dates)");
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
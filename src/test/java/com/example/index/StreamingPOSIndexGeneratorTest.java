package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import com.example.logging.ProgressTracker;

public class StreamingPOSIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-pos.txt";
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
        levelDbDir = tempDir.resolve("test-leveldb-pos").toFile();
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

        // Insert test sentences:
        // Doc 1: "The black cat sits quietly."
        // Doc 1: "It purrs softly."
        // Doc 2: "The black dog barks loudly."
        String[][] testWords = {
                // Document 1, Sentence 1
                { "1", "0", "0", "3", "The", "the", "DET" },
                { "1", "0", "4", "9", "quick", "quick", "ADJ" },
                { "1", "0", "10", "15", "brown", "brown", "ADJ" },
                { "1", "0", "16", "19", "fox", "fox", "NOUN" },
                { "1", "0", "20", "26", "jumps", "jump", "VERB" },
                // Document 1, Sentence 2
                { "1", "1", "27", "32", "over", "over", "ADP" },
                { "1", "1", "33", "36", "the", "the", "DET" },
                { "1", "1", "37", "41", "lazy", "lazy", "ADJ" },
                { "1", "1", "42", "45", "dog", "dog", "NOUN" },
                // Document 2, Sentence 1
                { "2", "0", "0", "2", "It", "it", "PRON" },
                { "2", "0", "3", "6", "was", "be", "AUX" },
                { "2", "0", "7", "8", "a", "a", "DET" },
                { "2", "0", "9", "13", "dark", "dark", "ADJ" },
                { "2", "0", "14", "19", "night", "night", "NOUN" }
        };

        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String[] word : testWords) {
                pstmt.setInt(1, Integer.parseInt(word[0]));
                pstmt.setInt(2, Integer.parseInt(word[1]));
                pstmt.setInt(3, Integer.parseInt(word[2]));
                pstmt.setInt(4, Integer.parseInt(word[3]));
                pstmt.setString(5, word[4]);
                pstmt.setString(6, word[5]);
                pstmt.setString(7, word[6]);
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
    public void testBasicPOSIndexing() throws Exception {
        // Create and run POS indexer
        try (StreamingPOSIndexGenerator indexer = new StreamingPOSIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Check that all POS tags are indexed
            String[] expectedTags = {"noun", "verb", "adj", "det", "adp", "pron", "aux"};
            for (String tag : expectedTags) {
                byte[] value = db.get(bytes(tag));
                assertNotNull(value, "POS tag " + tag + " should be indexed");
                
                PositionList positions = PositionList.deserialize(value);
                assertTrue(positions.size() > 0, "Should have positions for " + tag);
            }
            
            // Verify NOUN has multiple positions
            PositionList nounPositions = PositionList.deserialize(db.get(bytes("noun")));
            assertEquals(3, nounPositions.size(), "Should have three NOUN positions");
            
            // Verify DET is indexed despite being a stopword
            PositionList detPositions = PositionList.deserialize(db.get(bytes("det")));
            assertEquals(3, detPositions.size(), "Should have three DET positions");
        }
    }

    @Test
    public void testCaseNormalization() throws Exception {
        // Insert document record for mixed case test data
        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO documents (document_id, timestamp) VALUES (?, ?)")) {
            pstmt.setInt(1, 3);
            pstmt.setString(2, "2024-01-28");
            pstmt.executeUpdate();
        }

        // Insert mixed case POS tags
        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            Object[][] mixedCaseWords = {
                { 3, 0, 0, 5, "Hello", "hello", "NOUN" },
                { 3, 0, 6, 11, "World", "world", "noun" },
                { 3, 0, 12, 14, "is", "be", "Verb" },
                { 3, 0, 15, 20, "great", "great", "ADJ" }
            };

            for (Object[] word : mixedCaseWords) {
                pstmt.setInt(1, (Integer) word[0]);
                pstmt.setInt(2, (Integer) word[1]);
                pstmt.setInt(3, (Integer) word[2]);
                pstmt.setInt(4, (Integer) word[3]);
                pstmt.setString(5, (String) word[4]);
                pstmt.setString(6, (String) word[5]);
                pstmt.setString(7, (String) word[6]);
                pstmt.executeUpdate();
            }
        }

        // Generate index
        try (StreamingPOSIndexGenerator indexer = new StreamingPOSIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Verify case normalization
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Check that different cases of NOUN are merged
            PositionList nounPositions = PositionList.deserialize(db.get(bytes("noun")));
            assertEquals(5, nounPositions.size(), "Should have five NOUN positions (3 from setup + 2 from mixed case)");

            // Check that different cases of VERB are merged
            PositionList verbPositions = PositionList.deserialize(db.get(bytes("verb")));
            assertEquals(2, verbPositions.size(), "Should have two VERB positions (1 from setup + 1 from mixed case)");
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import com.example.logging.ProgressTracker;

public class DependencyIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-dep.txt";
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
        levelDbDir = tempDir.resolve("test-leveldb-dep").toFile();
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

        // Insert test dependencies:
        // "The quick brown fox jumps over the lazy dog"
        String[][] testDeps = {
            // Document 1, Sentence 1
            { "1", "0", "fox", "quick", "amod", "0", "15" },
            { "1", "0", "fox", "brown", "amod", "16", "30" },
            { "1", "0", "jumps", "fox", "nsubj", "31", "45" },
            { "1", "0", "jumps", "over", "advmod", "46", "60" },
            // Document 2, Sentence 1
            { "2", "0", "cat", "black", "amod", "0", "15" },
            { "2", "0", "sits", "cat", "nsubj", "16", "30" },
            { "2", "0", "sits", "quietly", "advmod", "31", "45" }
        };

        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO dependencies (document_id, sentence_id, head_token, dependent_token, relation, begin_char, end_char) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String[] dep : testDeps) {
                pstmt.setInt(1, Integer.parseInt(dep[0]));
                pstmt.setInt(2, Integer.parseInt(dep[1]));
                pstmt.setString(3, dep[2]);
                pstmt.setString(4, dep[3]);
                pstmt.setString(5, dep[4]);
                pstmt.setInt(6, Integer.parseInt(dep[5]));
                pstmt.setInt(7, Integer.parseInt(dep[6]));
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
    public void testBasicDependencyIndexing() throws Exception {
        // Create and run dependency indexer
        try (DependencyIndexGenerator indexer = new DependencyIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Test amod relations
            String key = "fox" + IndexGenerator.DELIMITER + "amod" + IndexGenerator.DELIMITER + "quick";
            PositionList positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for fox-amod->quick");
            assertEquals(1, positions.size(), "Should have one position for fox-amod->quick");

            key = "fox" + IndexGenerator.DELIMITER + "amod" + IndexGenerator.DELIMITER + "brown";
            positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for fox-amod->brown");
            assertEquals(1, positions.size(), "Should have one position for fox-amod->brown");

            // Test nsubj relations
            key = "jumps" + IndexGenerator.DELIMITER + "nsubj" + IndexGenerator.DELIMITER + "fox";
            positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for jumps-nsubj->fox");
            assertEquals(1, positions.size(), "Should have one position for jumps-nsubj->fox");

            // Test advmod relations
            key = "jumps" + IndexGenerator.DELIMITER + "advmod" + IndexGenerator.DELIMITER + "over";
            positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for jumps-advmod->over");
            assertEquals(1, positions.size(), "Should have one position for jumps-advmod->over");
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

        // Insert mixed case dependencies
        String[][] mixedCaseDeps = {
            { "3", "0", "FOX", "QUICK", "AMOD", "0", "15" },
            { "3", "0", "Fox", "Quick", "amod", "16", "30" }
        };

        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO dependencies (document_id, sentence_id, head_token, dependent_token, relation, begin_char, end_char) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String[] dep : mixedCaseDeps) {
                pstmt.setInt(1, Integer.parseInt(dep[0]));
                pstmt.setInt(2, Integer.parseInt(dep[1]));
                pstmt.setString(3, dep[2]);
                pstmt.setString(4, dep[3]);
                pstmt.setString(5, dep[4]);
                pstmt.setInt(6, Integer.parseInt(dep[5]));
                pstmt.setInt(7, Integer.parseInt(dep[6]));
                pstmt.executeUpdate();
            }
        }

        // Generate index
        try (DependencyIndexGenerator indexer = new DependencyIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Verify case normalization
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            String key = "fox" + IndexGenerator.DELIMITER + "amod" + IndexGenerator.DELIMITER + "quick";
            PositionList positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for fox-amod->quick");
            assertEquals(3, positions.size(), "Should have three positions for fox-amod->quick (1 from setup + 2 from mixed case)");
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
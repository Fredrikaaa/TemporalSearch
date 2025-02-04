package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import com.example.logging.ProgressTracker;

public class HypernymIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-hypernym.txt";
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
        levelDbDir = tempDir.resolve("test-leveldb-hypernym").toFile();
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

        // Insert test sentences with hypernym relations:
        // "Animals such as cats and dogs"
        // "Fruits including apples and oranges"
        String[][] testWords = {
            // Document 1, Sentence 1
            { "1", "0", "0", "7", "Animals", "animal", "NOUN" },
            { "1", "0", "8", "16", "such as", "such_as", "ADP" },
            { "1", "0", "17", "21", "cats", "cat", "NOUN" },
            { "1", "0", "22", "25", "and", "and", "CCONJ" },
            { "1", "0", "26", "30", "dogs", "dog", "NOUN" },
            // Document 2, Sentence 1
            { "2", "0", "0", "6", "Fruits", "fruit", "NOUN" },
            { "2", "0", "7", "16", "including", "including", "VERB" },
            { "2", "0", "17", "23", "apples", "apple", "NOUN" },
            { "2", "0", "24", "27", "and", "and", "CCONJ" },
            { "2", "0", "28", "35", "oranges", "orange", "NOUN" }
        };

        // Insert annotations
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

        // Insert dependencies
        String[][] testDeps = {
            // Document 1: Animals such_as cats, Animals such_as dogs
            { "1", "0", "Animals", "cats", "nmod:such_as", "0", "17" },
            { "1", "0", "Animals", "dogs", "nmod:such_as", "0", "26" },
            // Document 2: Fruits including apples, Fruits including oranges
            { "2", "0", "Fruits", "apples", "nmod:including", "0", "17" },
            { "2", "0", "Fruits", "oranges", "nmod:including", "0", "28" }
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
    public void testBasicHypernymIndexing() throws Exception {
        // Create and run hypernym indexer
        try (HypernymIndexGenerator indexer = new HypernymIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Test animal hypernyms
            String animalKey = "animal" + IndexGenerator.DELIMITER + "cat";
            PositionList catPositions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(animalKey))));
            assertNotNull(catPositions, "Should have positions for animal->cat");
            assertEquals(1, catPositions.size(), "Should have one position for animal->cat");

            animalKey = "animal" + IndexGenerator.DELIMITER + "dog";
            PositionList dogPositions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(animalKey))));
            assertNotNull(dogPositions, "Should have positions for animal->dog");
            assertEquals(1, dogPositions.size(), "Should have one position for animal->dog");

            // Test fruit hypernyms
            String fruitKey = "fruit" + IndexGenerator.DELIMITER + "apple";
            PositionList applePositions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(fruitKey))));
            assertNotNull(applePositions, "Should have positions for fruit->apple");
            assertEquals(1, applePositions.size(), "Should have one position for fruit->apple");

            fruitKey = "fruit" + IndexGenerator.DELIMITER + "orange";
            PositionList orangePositions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(fruitKey))));
            assertNotNull(orangePositions, "Should have positions for fruit->orange");
            assertEquals(1, orangePositions.size(), "Should have one position for fruit->orange");
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

        // Insert mixed case annotations
        String[][] mixedCaseWords = {
            { "3", "0", "0", "7", "ANIMALS", "ANIMAL", "NOUN" },
            { "3", "0", "8", "16", "such as", "such_as", "ADP" },
            { "3", "0", "17", "21", "Cats", "Cat", "NOUN" }
        };

        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String[] word : mixedCaseWords) {
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

        // Insert mixed case dependency
        try (PreparedStatement pstmt = sqliteConn.prepareStatement(
                "INSERT INTO dependencies (document_id, sentence_id, head_token, dependent_token, relation, begin_char, end_char) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 3);
            pstmt.setInt(2, 0);
            pstmt.setString(3, "ANIMALS");
            pstmt.setString(4, "Cats");
            pstmt.setString(5, "nmod:such_as");
            pstmt.setInt(6, 0);
            pstmt.setInt(7, 21);
            pstmt.executeUpdate();
        }

        // Generate index
        try (HypernymIndexGenerator indexer = new HypernymIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Verify case normalization
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            String key = "animal" + IndexGenerator.DELIMITER + "cat";
            PositionList positions = PositionList.deserialize(db.get(bytes(KeyPrefixes.createPositionsKey(key))));
            assertNotNull(positions, "Should have positions for animal->cat");
            assertEquals(2, positions.size(), "Should have two positions for animal->cat (1 from setup + 1 from mixed case)");
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
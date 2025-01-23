package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;

public class BigramIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-bigram.txt";
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
        levelDbDir = tempDir.resolve("test-leveldb-bigram").toFile();
        if (levelDbDir.exists()) {
            deleteDirectory(levelDbDir);
        }
        levelDbDir.mkdir();

        // Insert test data
        setupTestData();
    }

    private void setupTestData() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert test documents
            stmt.execute("INSERT INTO documents VALUES (1, '2024-01-01T00:00:00Z')");
            stmt.execute("INSERT INTO documents VALUES (2, '2024-01-01T00:00:00Z')");

            // Insert test sentences:
            // Doc 1: "The black cat sits quietly."
            // Doc 1: "It purrs softly."
            // Doc 2: "The black dog barks loudly."
            String[][] testWords = {
                    // Document 1, Sentence 1
                    { "1", "0", "0", "3", "The", "the" },
                    { "1", "0", "4", "9", "black", "black" },
                    { "1", "0", "10", "13", "cat", "cat" },
                    { "1", "0", "14", "18", "sits", "sit" },
                    { "1", "0", "19", "26", "quietly", "quietly" },
                    // Document 1, Sentence 2
                    { "1", "1", "27", "29", "It", "it" },
                    { "1", "1", "30", "35", "purrs", "purr" },
                    { "1", "1", "36", "42", "softly", "softly" },
                    // Document 2, Sentence 1
                    { "2", "0", "0", "3", "The", "the" },
                    { "2", "0", "4", "9", "black", "black" },
                    { "2", "0", "10", "13", "dog", "dog" },
                    { "2", "0", "14", "19", "barks", "bark" },
                    { "2", "0", "20", "26", "loudly", "loudly" }
            };

            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] word : testWords) {
                pstmt.setInt(1, Integer.parseInt(word[0]));
                pstmt.setInt(2, Integer.parseInt(word[1]));
                pstmt.setInt(3, Integer.parseInt(word[2]));
                pstmt.setInt(4, Integer.parseInt(word[3]));
                pstmt.setString(5, word[4]);
                pstmt.setString(6, word[5]);
                pstmt.executeUpdate();
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        super.tearDown();
        new File(TEST_STOPWORDS_PATH).delete();
    }

    @Test
    public void testBasicBigramIndexing() throws Exception {
        // Create and run bigram indexer
        try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Test bigrams with stopwords
            verifyBigram(db, "the" + BaseIndexGenerator.DELIMITER + "black", 1, 0, 0, 9, 2); // Appears in both documents

            // Test regular bigrams
            verifyBigram(db, "black" + BaseIndexGenerator.DELIMITER + "cat", 1, 0, 4, 13, 1);
            verifyBigram(db, "cat" + BaseIndexGenerator.DELIMITER + "sit", 1, 0, 10, 18, 1);
            verifyBigram(db, "sit" + BaseIndexGenerator.DELIMITER + "quietly", 1, 0, 14, 26, 1);

            // Test bigrams in second document
            verifyBigram(db, "black" + BaseIndexGenerator.DELIMITER + "dog", 2, 0, 4, 13, 1);
            verifyBigram(db, "dog" + BaseIndexGenerator.DELIMITER + "bark", 2, 0, 10, 19, 1);
        }
    }

    @Test
    public void testSentenceBoundaries() throws Exception {
        try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Verify no bigram exists between sentences
            assertNull(db.get(bytes("quietly" + BaseIndexGenerator.DELIMITER + "it")),
                    "Bigram should not span sentence boundary");

            // Verify bigrams within second sentence exist
            verifyBigram(db, "it" + BaseIndexGenerator.DELIMITER + "purr", 1, 1, 27, 35, 1);
            verifyBigram(db, "purr" + BaseIndexGenerator.DELIMITER + "softly", 1, 1, 30, 42, 1);
        }
    }

    @Test
    public void testBigramFrequencies() throws Exception {
        // Add another occurrence of "black dog"
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents VALUES (3, '2024-01-01T00:00:00Z')");

            String[][] additionalWords = {
                    { "3", "0", "0", "5", "black", "black" },
                    { "3", "0", "6", "9", "dog", "dog" }
            };

            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] word : additionalWords) {
                pstmt.setInt(1, Integer.parseInt(word[0]));
                pstmt.setInt(2, Integer.parseInt(word[1]));
                pstmt.setInt(3, Integer.parseInt(word[2]));
                pstmt.setInt(4, Integer.parseInt(word[3]));
                pstmt.setString(5, word[4]);
                pstmt.setString(6, word[5]);
                pstmt.executeUpdate();
            }
        }

        try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Verify "black dog" appears twice
            verifyBigram(db, "black" + BaseIndexGenerator.DELIMITER + "dog", 2, 0, 4, 13, 2);
        }
    }

    private void verifyBigram(DB db, String bigramKey, int expectedDocId,
            int expectedSentenceId, int expectedBeginChar,
            int expectedEndChar, int expectedCount) throws IOException {
        byte[] data = db.get(bytes(bigramKey));
        assertNotNull(data, "Bigram should exist: " + bigramKey);

        PositionList positions = PositionList.deserialize(data);
        assertEquals(expectedCount, positions.size(),
                "Should have correct number of occurrences for: " + bigramKey);

        Position pos = positions.getPositions().get(0);
        assertEquals(expectedDocId, pos.getDocumentId(),
                "Document ID mismatch for: " + bigramKey);
        assertEquals(expectedSentenceId, pos.getSentenceId(),
                "Sentence ID mismatch for: " + bigramKey);
        assertEquals(expectedBeginChar, pos.getBeginPosition(),
                "Begin position mismatch for: " + bigramKey);
        assertEquals(expectedEndChar, pos.getEndPosition(),
                "End position mismatch for: " + bigramKey);
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

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}

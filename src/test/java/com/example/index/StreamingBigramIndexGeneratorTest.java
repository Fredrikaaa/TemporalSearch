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

public class StreamingBigramIndexGeneratorTest extends BaseIndexTest {
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
                { "1", "0", "4", "9", "black", "black", "ADJ" },
                { "1", "0", "10", "13", "cat", "cat", "NOUN" },
                { "1", "0", "14", "18", "sits", "sit", "VERB" },
                { "1", "0", "19", "26", "quietly", "quietly", "ADV" },
                // Document 1, Sentence 2
                { "1", "1", "27", "29", "It", "it", "PRON" },
                { "1", "1", "30", "35", "purrs", "purr", "VERB" },
                { "1", "1", "36", "42", "softly", "softly", "ADV" },
                // Document 2, Sentence 1
                { "2", "0", "0", "3", "The", "the", "DET" },
                { "2", "0", "4", "9", "black", "black", "ADJ" },
                { "2", "0", "10", "13", "dog", "dog", "NOUN" },
                { "2", "0", "14", "19", "barks", "bark", "VERB" },
                { "2", "0", "20", "26", "loudly", "loudly", "ADV" }
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
    public void testBasicIndexing() throws Exception {
        // Create and run bigram indexer
        try (StreamingBigramIndexGenerator indexer = new StreamingBigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Test bigrams with stopwords
            verifyBigram(db, "the" + StreamingIndexGenerator.DELIMITER + "black", 1, 0, 0, 9, 2); // Appears in both documents

            // Test regular bigrams
            verifyBigram(db, "black" + StreamingIndexGenerator.DELIMITER + "cat", 1, 0, 4, 13, 1);
            verifyBigram(db, "cat" + StreamingIndexGenerator.DELIMITER + "sit", 1, 0, 10, 18, 1);
            verifyBigram(db, "sit" + StreamingIndexGenerator.DELIMITER + "quietly", 1, 0, 14, 26, 1);

            // Test bigrams in second document
            verifyBigram(db, "black" + StreamingIndexGenerator.DELIMITER + "dog", 2, 0, 4, 13, 1);
            verifyBigram(db, "dog" + StreamingIndexGenerator.DELIMITER + "bark", 2, 0, 10, 19, 1);
        }
    }

    @Test
    public void testSentenceBoundaries() throws Exception {
        try (StreamingBigramIndexGenerator indexer = new StreamingBigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Verify no bigrams cross sentence boundaries
            assertNull(db.get(bytes("quietly" + StreamingIndexGenerator.DELIMITER + "it")),
                    "Bigram should not cross sentence boundary");
            assertNull(db.get(bytes("softly" + StreamingIndexGenerator.DELIMITER + "the")),
                    "Bigram should not cross sentence boundary");
        }
    }

    private void verifyBigram(DB db, String bigram, int expectedDocId, int expectedSentenceId,
            int expectedBeginChar, int expectedEndChar, int expectedCount) throws IOException {
        byte[] value = db.get(bytes(bigram));
        assertNotNull(value, "Bigram '" + bigram + "' should be indexed");
        
        PositionList positions = PositionList.deserialize(value);
        assertEquals(expectedCount, positions.size(),
                String.format("Bigram '%s' should appear %d time(s)", bigram, expectedCount));
        
        Position pos = positions.getPositions().get(0);
        assertEquals(expectedDocId, pos.getDocumentId());
        assertEquals(expectedSentenceId, pos.getSentenceId());
        assertEquals(expectedBeginChar, pos.getBeginPosition());
        assertEquals(expectedEndChar, pos.getEndPosition());
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
} 
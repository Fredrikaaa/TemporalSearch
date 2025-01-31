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

public class StreamingTrigramIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-trigram.txt";
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
        levelDbDir = tempDir.resolve("test-leveldb-trigram").toFile();
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
                { "1", "0", "27", "30", "now", "now", "ADV" },
                // Document 1, Sentence 2
                { "1", "1", "31", "33", "It", "it", "PRON" },
                { "1", "1", "34", "39", "purrs", "purr", "VERB" },
                { "1", "1", "40", "44", "very", "very", "ADV" },
                { "1", "1", "45", "51", "softly", "softly", "ADV" },
                { "1", "1", "52", "57", "today", "today", "ADV" },
                // Document 2, Sentence 1
                { "2", "0", "0", "3", "The", "the", "DET" },
                { "2", "0", "4", "9", "black", "black", "ADJ" },
                { "2", "0", "10", "13", "cat", "cat", "NOUN" },
                { "2", "0", "14", "18", "runs", "run", "VERB" },
                { "2", "0", "19", "26", "quickly", "quickly", "ADV" },
                { "2", "0", "27", "31", "away", "away", "ADV" }
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
    public void testBasicTrigramIndexing() throws Exception {
        // Create and run trigram indexer
        try (StreamingTrigramIndexGenerator indexer = new StreamingTrigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Test trigrams with stopwords
            verifyTrigram(db, "the" + StreamingIndexGenerator.DELIMITER + "black" + 
                StreamingIndexGenerator.DELIMITER + "cat", 1, 0, 0, 13, 2);

            // Test regular trigrams in first document
            verifyTrigram(db, "black" + StreamingIndexGenerator.DELIMITER + "cat" + 
                StreamingIndexGenerator.DELIMITER + "sit", 1, 0, 4, 18, 1);
            verifyTrigram(db, "cat" + StreamingIndexGenerator.DELIMITER + "sit" + 
                StreamingIndexGenerator.DELIMITER + "quietly", 1, 0, 10, 26, 1);
            verifyTrigram(db, "sit" + StreamingIndexGenerator.DELIMITER + "quietly" + 
                StreamingIndexGenerator.DELIMITER + "now", 1, 0, 14, 30, 1);

            // Test trigrams in second document
            verifyTrigram(db, "black" + StreamingIndexGenerator.DELIMITER + "cat" + 
                StreamingIndexGenerator.DELIMITER + "run", 2, 0, 4, 18, 1);
            verifyTrigram(db, "cat" + StreamingIndexGenerator.DELIMITER + "run" + 
                StreamingIndexGenerator.DELIMITER + "quickly", 2, 0, 10, 26, 1);
        }
    }

    @Test
    public void testSentenceBoundaries() throws Exception {
        try (StreamingTrigramIndexGenerator indexer = new StreamingTrigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, sqliteConn, new ProgressTracker())) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Verify no trigrams cross sentence boundaries
            assertNull(db.get(bytes(KeyPrefixes.createPositionsKey("quietly" + StreamingIndexGenerator.DELIMITER + "now" + 
                StreamingIndexGenerator.DELIMITER + "it"))), "Trigram should not cross sentence boundary");
            assertNull(db.get(bytes(KeyPrefixes.createPositionsKey("now" + StreamingIndexGenerator.DELIMITER + "it" + 
                StreamingIndexGenerator.DELIMITER + "purr"))), "Trigram should not cross sentence boundary");
        }
    }

    private void verifyTrigram(DB db, String trigram, int expectedDocId, int expectedSentenceId,
            int expectedBeginChar, int expectedEndChar, int expectedCount) throws IOException {
        byte[] value = db.get(bytes(KeyPrefixes.createPositionsKey(trigram)));
        assertNotNull(value, "Trigram '" + trigram + "' should be indexed");
        
        PositionList positions = PositionList.deserialize(value);
        assertEquals(expectedCount, positions.size(),
                String.format("Trigram '%s' should appear %d time(s)", trigram, expectedCount));
        
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
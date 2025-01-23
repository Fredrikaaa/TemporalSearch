package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import java.util.*;

public class UnigramIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords.txt";
    private File levelDbDir;

    @BeforeEach
    @Override
    void setUp() throws Exception {
        super.setUp();
        
        // Create test stopwords file
        createStopwordsFile();

        // Set up LevelDB directory
        levelDbDir = tempDir.resolve("test-leveldb").toFile();
        if (levelDbDir.exists()) {
            deleteDirectory(levelDbDir);
        }
        levelDbDir.mkdir();
        
        // Insert test data
        createTestData();
    }

    private void createStopwordsFile() throws IOException {
        try (PrintWriter writer = new PrintWriter(TEST_STOPWORDS_PATH)) {
            writer.println("the");
            writer.println("a");
            writer.println("is");
        }
    }

    private void createTestData() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert test documents with proper ISO-8601 timestamps
            stmt.execute("INSERT INTO documents VALUES (1, '2024-01-01T00:00:00Z')");
            stmt.execute("INSERT INTO documents VALUES (2, '2024-01-02T00:00:00Z')");

            // Insert test annotations
            String[][] testWords = {
                    // doc_id, sentence_id, begin, end, token, lemma
                    { "1", "0", "0", "3", "The", "the" },
                    { "1", "0", "4", "7", "cat", "cat" },
                    { "1", "0", "8", "12", "sits", "sit" },
                    { "2", "0", "0", "3", "The", "the" },
                    { "2", "0", "4", "7", "dog", "dog" },
                    { "2", "0", "8", "13", "barks", "bark" }
            };

            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                            "VALUES (?, ?, ?, ?, ?, ?, 'NN')");

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
        // Create and run unigram indexer
        try (UnigramIndexGenerator indexer = new UnigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Check for expected words
            assertWordExists(db, "cat");
            assertWordExists(db, "dog");
            assertWordExists(db, "sit");
            assertWordExists(db, "bark");

            // Verify stopword was excluded
            assertNull(db.get(bytes("the")), "Stopword 'the' should not be indexed");

            // Verify position data for 'cat'
            byte[] catData = db.get(bytes("cat"));
            PositionList catPositions = PositionList.deserialize(catData);
            List<Position> positions = catPositions.getPositions();

            assertEquals(1, positions.size(), "Should have one occurrence of 'cat'");
            Position catPos = positions.get(0);
            assertEquals(1, catPos.getDocumentId(), "Wrong document ID for 'cat'");
            assertEquals(0, catPos.getSentenceId(), "Wrong sentence ID for 'cat'");
            assertEquals(4, catPos.getBeginPosition(), "Wrong begin position for 'cat'");
            assertEquals(7, catPos.getEndPosition(), "Wrong end position for 'cat'");
        }
    }

    @Test
    public void testBatchProcessing() throws Exception {
        // Insert many entries to test batch processing
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert 1000 test entries
            for (int i = 0; i < 1000; i++) {
                stmt.execute(String.format(
                        "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma) " +
                                "VALUES (1, 0, %d, %d, 'word%d', 'word%d')",
                        i * 5, i * 5 + 4, i, i));
            }
        }

        // Run indexer with small batch size
        try (UnigramIndexGenerator indexer = new UnigramIndexGenerator(
                levelDbDir.getPath(), TEST_STOPWORDS_PATH, 10, sqliteConn)) {
            indexer.generateIndex();
        }

        // Verify results
        Options options = new Options();
        try (DB db = factory.open(levelDbDir, options)) {
            // Check random words from different batches
            assertWordExists(db, "word0");
            assertWordExists(db, "word499");
            assertWordExists(db, "word999");
        }
    }

    private void assertWordExists(DB db, String word) {
        assertNotNull(db.get(bytes(word)),
                String.format("Word '%s' should exist in index", word));
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}

package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import java.time.LocalDate;

public class BigramIndexGeneratorTest {
    private static final String TEST_DB_PATH = "test-leveldb-bigram";
    private static final String TEST_SQLITE_PATH = "test-sqlite-bigram.db";
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-bigram.txt";
    private Connection sqliteConn;
    private File levelDbDir;

    @BeforeEach
    public void setup() throws Exception {
        // Create test stopwords file - we'll still create it even though we're
        // including stopwords
        // in bigrams, as it's used by the base generator infrastructure
        try (PrintWriter writer = new PrintWriter(TEST_STOPWORDS_PATH)) {
            writer.println("the");
            writer.println("a");
            writer.println("is");
        }

        // Create test SQLite database
        sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + TEST_SQLITE_PATH);
        try (Statement stmt = sqliteConn.createStatement()) {
            // Create tables
            stmt.execute("CREATE TABLE documents (document_id INTEGER PRIMARY KEY, timestamp TEXT)");
            stmt.execute("""
                        CREATE TABLE annotations (
                            annotation_id INTEGER PRIMARY KEY,
                            document_id INTEGER,
                            sentence_id INTEGER,
                            begin_char INTEGER,
                            end_char INTEGER,
                            token TEXT,
                            lemma TEXT,
                            pos TEXT
                        )
                    """);

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

        // Set up LevelDB directory
        levelDbDir = new File(TEST_DB_PATH);
        if (levelDbDir.exists()) {
            deleteDirectory(levelDbDir);
        }
        levelDbDir.mkdir();
    }

    @Test
    public void testBasicBigramIndexing() throws Exception {
        // Create and run bigram indexer
        try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Test bigrams with stopwords
            verifyBigram(db, "the\u0000black", 1, 0, 0, 9, 2); // Appears in both documents

            // Test regular bigrams
            verifyBigram(db, "black\u0000cat", 1, 0, 4, 13, 1);
            verifyBigram(db, "cat\u0000sit", 1, 0, 10, 18, 1);
            verifyBigram(db, "sit\u0000quietly", 1, 0, 14, 26, 1);

            // Test bigrams in second document
            verifyBigram(db, "black\u0000dog", 2, 0, 4, 13, 1);
            verifyBigram(db, "dog\u0000bark", 2, 0, 10, 19, 1);
        }
    }

    @Test
    public void testSentenceBoundaries() throws Exception {
        try (BigramIndexGenerator indexer = new BigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify no bigram exists between sentences
            assertNull(db.get(bytes("quietly\u0000it")),
                    "Bigram should not span sentence boundary");

            // Verify bigrams within second sentence exist
            verifyBigram(db, "it\u0000purr", 1, 1, 27, 35, 1);
            verifyBigram(db, "purr\u0000softly", 1, 1, 30, 42, 1);
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
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify "black dog" appears twice
            verifyBigram(db, "black\u0000dog", 2, 0, 4, 13, 2);
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

    @AfterEach
    public void cleanup() throws Exception {
        if (sqliteConn != null && !sqliteConn.isClosed()) {
            sqliteConn.close();
        }
        new File(TEST_SQLITE_PATH).delete();
        new File(TEST_STOPWORDS_PATH).delete();
        deleteDirectory(levelDbDir);
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}

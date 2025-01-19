package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import java.time.LocalDate;

public class POSIndexGeneratorTest {
    private static final String TEST_DB_PATH = "test-leveldb-pos";
    private static final String TEST_SQLITE_PATH = "test-sqlite-pos.db";
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-pos.txt";
    private Connection sqliteConn;
    private File levelDbDir;

    @BeforeEach
    public void setup() throws Exception {
        // Create test stopwords file (not used for POS indexing but required by base class)
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
            stmt.execute("CREATE TABLE annotations (" +
                "annotation_id INTEGER PRIMARY KEY," +
                "document_id INTEGER," +
                "sentence_id INTEGER," +
                "begin_char INTEGER," +
                "end_char INTEGER," +
                "token TEXT," +
                "lemma TEXT," +
                "pos TEXT" +
                ")");

            // Insert test documents
            stmt.execute("INSERT INTO documents VALUES (1, '2024-01-01T00:00:00Z')");
            stmt.execute("INSERT INTO documents VALUES (2, '2024-01-01T00:00:00Z')");

            // Insert test sentences with POS tags:
            // Doc 1: "The black cat sits quietly."
            // Doc 2: "The black dog barks loudly."
            String[][] testWords = {
                    // Document 1
                    { "1", "0", "0", "3", "The", "DT" },
                    { "1", "0", "4", "9", "black", "JJ" },
                    { "1", "0", "10", "13", "cat", "NN" },
                    { "1", "0", "14", "18", "sits", "VBZ" },
                    { "1", "0", "19", "26", "quietly", "RB" },
                    // Document 2
                    { "2", "0", "0", "3", "The", "DT" },
                    { "2", "0", "4", "9", "black", "JJ" },
                    { "2", "0", "10", "13", "dog", "NN" },
                    { "2", "0", "14", "19", "barks", "VBZ" },
                    { "2", "0", "20", "26", "loudly", "RB" }
            };

            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, pos) " +
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
    public void testBasicPOSIndexing() throws Exception {
        // Create and run POS indexer
        try (POSIndexGenerator indexer = new POSIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Test determiners (DT)
            verifyPOSTag(db, "dt", 1, 0, 0, 3, 2); // Should appear twice (The, The)

            // Test adjectives (JJ)
            verifyPOSTag(db, "jj", 1, 0, 4, 9, 2); // Should appear twice (black, black)

            // Test nouns (NN)
            verifyPOSTag(db, "nn", 1, 0, 10, 13, 2); // Should appear twice (cat, dog)

            // Test verbs (VBZ)
            verifyPOSTag(db, "vbz", 1, 0, 14, 18, 2); // Should appear twice (sits, barks)

            // Test adverbs (RB)
            verifyPOSTag(db, "rb", 1, 0, 19, 26, 2); // Should appear twice (quietly, loudly)
        }
    }

    @Test
    public void testNullPOSHandling() throws Exception {
        // Add entry with null POS
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, pos) " +
                    "VALUES (1, 1, 27, 30, 'and', NULL)");
        }

        try (POSIndexGenerator indexer = new POSIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify that entries with valid POS tags are still indexed
            verifyPOSTag(db, "dt", 1, 0, 0, 3, 2);
            
            // Verify that null POS tags are not indexed
            assertNull(db.get(bytes("null")), "Null POS tag should not be indexed");
        }
    }

    @Test
    public void testCaseInsensitivity() throws Exception {
        // Add entries with mixed case POS tags
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, pos) " +
                    "VALUES (1, 1, 27, 30, 'test', 'NN')");
            stmt.execute("INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, pos) " +
                    "VALUES (1, 1, 31, 34, 'test', 'nn')");
        }

        try (POSIndexGenerator indexer = new POSIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify that POS tags are case-insensitive
            byte[] data = db.get(bytes("nn"));
            assertNotNull(data, "POS tag should be indexed case-insensitively");
            
            PositionList positions = PositionList.deserialize(data);
            assertEquals(4, positions.size(), "Should have all occurrences regardless of case");
        }
    }

    private void verifyPOSTag(DB db, String posTag, int expectedDocId,
            int expectedSentenceId, int expectedBeginChar,
            int expectedEndChar, int expectedCount) throws IOException {
        byte[] data = db.get(bytes(posTag));
        assertNotNull(data, "POS tag should exist: " + posTag);

        PositionList positions = PositionList.deserialize(data);
        assertEquals(expectedCount, positions.size(),
                "Should have correct number of occurrences for: " + posTag);

        Position pos = positions.getPositions().get(0);
        assertEquals(expectedDocId, pos.getDocumentId(),
                "Document ID mismatch for: " + posTag);
        assertEquals(expectedSentenceId, pos.getSentenceId(),
                "Sentence ID mismatch for: " + posTag);
        assertEquals(expectedBeginChar, pos.getBeginPosition(),
                "Begin position mismatch for: " + posTag);
        assertEquals(expectedEndChar, pos.getEndPosition(),
                "End position mismatch for: " + posTag);
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
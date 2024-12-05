package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;

public class TrigramIndexGeneratorTest {
    private static final String TEST_DB_PATH = "test-leveldb-trigram";
    private static final String TEST_SQLITE_PATH = "test-sqlite-trigram.db";
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-trigram.txt";
    private Connection sqliteConn;
    private File levelDbDir;

    @BeforeEach
    public void setup() throws Exception {
        // Create test stopwords file
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
            // Doc 1: "The black cat sits quietly now"
            // Doc 1: "It purrs very softly today"
            // Doc 2: "The black cat runs quickly away"
            String[][] testWords = {
                    // Document 1, Sentence 1
                    { "1", "0", "0", "3", "The", "the" },
                    { "1", "0", "4", "9", "black", "black" },
                    { "1", "0", "10", "13", "cat", "cat" },
                    { "1", "0", "14", "18", "sits", "sit" },
                    { "1", "0", "19", "26", "quietly", "quietly" },
                    { "1", "0", "27", "30", "now", "now" },
                    // Document 1, Sentence 2
                    { "1", "1", "31", "33", "It", "it" },
                    { "1", "1", "34", "39", "purrs", "purr" },
                    { "1", "1", "40", "44", "very", "very" },
                    { "1", "1", "45", "51", "softly", "softly" },
                    { "1", "1", "52", "57", "today", "today" },
                    // Document 2, Sentence 1
                    { "2", "0", "0", "3", "The", "the" },
                    { "2", "0", "4", "9", "black", "black" },
                    { "2", "0", "10", "13", "cat", "cat" },
                    { "2", "0", "14", "18", "runs", "run" },
                    { "2", "0", "19", "26", "quickly", "quickly" },
                    { "2", "0", "27", "31", "away", "away" }
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
    public void testBasicTrigramIndexing() throws Exception {
        // Create and run trigram indexer
        try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        // Open LevelDB and verify contents
        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Test trigrams with stopwords
            verifyTrigram(db, "the\u0000black\u0000cat", 1, 0, 0, 13, 2); // Appears in both docs

            // Test regular trigrams in first document
            verifyTrigram(db, "black\u0000cat\u0000sit", 1, 0, 4, 18, 1);
            verifyTrigram(db, "cat\u0000sit\u0000quietly", 1, 0, 10, 26, 1);
            verifyTrigram(db, "sit\u0000quietly\u0000now", 1, 0, 14, 30, 1);

            // Test regular trigrams in second document
            verifyTrigram(db, "black\u0000cat\u0000run", 2, 0, 4, 18, 1);
            verifyTrigram(db, "cat\u0000run\u0000quickly", 2, 0, 10, 26, 1);
        }
    }

    @Test
    public void testSentenceBoundaries() throws Exception {
        try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify no trigram exists between sentences
            assertNull(db.get(bytes("quietly\u0000now\u0000it")),
                    "Trigram should not span sentence boundary");

            // Verify trigrams within second sentence exist
            verifyTrigram(db, "it\u0000purr\u0000very", 1, 1, 31, 44, 1);
            verifyTrigram(db, "purr\u0000very\u0000softly", 1, 1, 34, 51, 1);
        }
    }

    private void verifyTrigram(DB db, String trigramKey, int expectedDocId,
            int expectedSentenceId, int expectedBeginChar,
            int expectedEndChar, int expectedCount) throws IOException {
        byte[] data = db.get(bytes(trigramKey));
        assertNotNull(data, "Trigram should exist: " + trigramKey);

        PositionList positions = PositionList.deserialize(data);
        assertEquals(expectedCount, positions.size(),
                "Should have correct number of occurrences for: " + trigramKey);

        Position pos = positions.getPositions().get(0);
        assertEquals(expectedDocId, pos.getDocumentId(),
                "Document ID mismatch for: " + trigramKey);
        assertEquals(expectedSentenceId, pos.getSentenceId(),
                "Sentence ID mismatch for: " + trigramKey);
        assertEquals(expectedBeginChar, pos.getBeginPosition(),
                "Begin position mismatch for: " + trigramKey);
        assertEquals(expectedEndChar, pos.getEndPosition(),
                "End position mismatch for: " + trigramKey);
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
    public void testOverlappingTrigrams() throws Exception {
        // Test case for overlapping trigrams within a sentence
        // This helps verify that we're correctly generating all possible trigrams
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert a longer sentence with overlapping trigrams
            stmt.execute("INSERT INTO documents VALUES (3, '2024-01-01T00:00:00Z')");

            // Sentence: "The quick brown fox jumps"
            String[][] words = {
                    { "3", "0", "0", "3", "The", "the" },
                    { "3", "0", "4", "9", "quick", "quick" },
                    { "3", "0", "10", "15", "brown", "brown" },
                    { "3", "0", "16", "19", "fox", "fox" },
                    { "3", "0", "20", "25", "jumps", "jump" }
            };

            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] word : words) {
                pstmt.setInt(1, Integer.parseInt(word[0]));
                pstmt.setInt(2, Integer.parseInt(word[1]));
                pstmt.setInt(3, Integer.parseInt(word[2]));
                pstmt.setInt(4, Integer.parseInt(word[3]));
                pstmt.setString(5, word[4]);
                pstmt.setString(6, word[5]);
                pstmt.executeUpdate();
            }
        }

        try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify all overlapping trigrams are present
            verifyTrigram(db, "the\u0000quick\u0000brown", 3, 0, 0, 15, 1);
            verifyTrigram(db, "quick\u0000brown\u0000fox", 3, 0, 4, 19, 1);
            verifyTrigram(db, "brown\u0000fox\u0000jump", 3, 0, 10, 25, 1);
        }
    }

    @Test
    public void testTrigramFrequencies() throws Exception {
        // Add another occurrence of a common three-word phrase
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents VALUES (3, '2024-01-01T00:00:00Z')");

            // Adding another "the black cat" sequence
            String[][] words = {
                    { "3", "0", "0", "3", "The", "the" },
                    { "3", "0", "4", "9", "black", "black" },
                    { "3", "0", "10", "13", "cat", "cat" },
                    { "3", "0", "14", "20", "sleeps", "sleep" }
            };

            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            for (String[] word : words) {
                pstmt.setInt(1, Integer.parseInt(word[0]));
                pstmt.setInt(2, Integer.parseInt(word[1]));
                pstmt.setInt(3, Integer.parseInt(word[2]));
                pstmt.setInt(4, Integer.parseInt(word[3]));
                pstmt.setString(5, word[4]);
                pstmt.setString(6, word[5]);
                pstmt.executeUpdate();
            }
        }

        try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify the trigram appears three times (twice in original data, once in new
            // data)
            verifyTrigram(db, "the\u0000black\u0000cat", 1, 0, 0, 13, 3);
        }
    }

    @Test
    public void testEmptySentence() throws Exception {
        // Test handling of empty or single-word sentences
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents VALUES (3, '2024-01-01T00:00:00Z')");

            // Insert a single-word sentence
            PreparedStatement pstmt = sqliteConn.prepareStatement(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");

            pstmt.setInt(1, 3);
            pstmt.setInt(2, 0);
            pstmt.setInt(3, 0);
            pstmt.setInt(4, 5);
            pstmt.setString(5, "Hello");
            pstmt.setString(6, "hello");
            pstmt.executeUpdate();
        }

        try (TrigramIndexGenerator indexer = new TrigramIndexGenerator(
                TEST_DB_PATH, TEST_STOPWORDS_PATH, 100, sqliteConn)) {
            indexer.generateIndex();
        }

        Options options = new Options();
        try (DB db = factory.open(new File(TEST_DB_PATH), options)) {
            // Verify no trigrams were created for the single-word sentence
            DBIterator iterator = db.iterator();
            iterator.seekToFirst();
            while (iterator.hasNext()) {
                String key = new String(iterator.peekNext().getKey());
                assertFalse(key.startsWith("hello\u0000"),
                        "No trigrams should start with the single word");
                iterator.next();
            }
        }
    }

    @AfterEach
    public void cleanup() throws Exception {
        // Close SQLite connection
        if (sqliteConn != null && !sqliteConn.isClosed()) {
            sqliteConn.close();
        }

        // Delete test files
        new File(TEST_SQLITE_PATH).delete();
        new File(TEST_STOPWORDS_PATH).delete();
        deleteDirectory(levelDbDir);
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}

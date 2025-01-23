package com.example.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HypernymIndexGeneratorTest extends BaseIndexTest {
    private static final String TEST_STOPWORDS_PATH = "test-stopwords-hypernym.txt";
    private HypernymIndexGenerator indexer;

    @Override
    protected void createBasicTables() throws Exception {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("""
                CREATE TABLE documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL
                )
            """);

            stmt.execute("""
                CREATE TABLE annotations (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    token TEXT,
                    lemma TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);

            stmt.execute("""
                CREATE TABLE dependencies (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    begin_char INTEGER,
                    end_char INTEGER,
                    head_token TEXT,
                    dependent_token TEXT,
                    relation TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test documents
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-15T10:00:00Z')");
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (2, '2024-01-16T10:00:00Z')");
            
            // Insert test annotations (tokens and lemmas)
            stmt.execute("""
                INSERT INTO annotations (document_id, sentence_id, token, lemma) VALUES
                (1, 1, 'animals', 'animal'),
                (1, 1, 'such', 'such'),
                (1, 1, 'as', 'as'),
                (1, 1, 'cats', 'cat'),
                (1, 1, 'and', 'and'),
                (1, 1, 'dogs', 'dog'),
                (2, 1, 'colors', 'color'),
                (2, 1, 'including', 'including'),
                (2, 1, 'red', 'red'),
                (2, 1, 'and', 'and'),
                (2, 1, 'blue', 'blue')
            """);
            
            // Insert test dependencies
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, begin_char, end_char, head_token, dependent_token, relation) VALUES
                (1, 1, 0, 20, 'animals', 'cats', 'nmod:such_as'),
                (1, 1, 25, 40, 'animals', 'dogs', 'nmod:such_as'),
                (2, 1, 0, 20, 'colors', 'red', 'nmod:including'),
                (2, 1, 25, 40, 'colors', 'blue', 'nmod:including')
            """);
        }
    }

    @BeforeEach
    void setUpIndexer() throws Exception {
        // Create test stopwords file
        try (PrintWriter writer = new PrintWriter(TEST_STOPWORDS_PATH)) {
            writer.println("the");
            writer.println("a");
            writer.println("is");
            writer.println("such");
            writer.println("as");
            writer.println("and");
            writer.println("including");
        }
        
        // Initialize indexer
        String levelDbPath = tempDir.resolve("leveldb").toString();
        indexer = new HypernymIndexGenerator(levelDbPath, TEST_STOPWORDS_PATH, 100, sqliteConn, "dependencies", 4);
    }

    @AfterEach
    void tearDownIndexer() throws Exception {
        if (indexer != null) {
            indexer.close();
        }
        // Delete test files
        new File(TEST_STOPWORDS_PATH).delete();
    }

    @Test
    void testBasicHypernymIndexing() throws Exception {
        // Generate index
        indexer.generateIndex();
        
        // Check for valid hypernym-hyponym pairs
        assertTrue(indexer.hasKey("animal\0cat")); // First pair
        assertTrue(indexer.hasKey("animal\0dog")); // Second pair
        assertTrue(indexer.hasKey("color\0red")); // Third pair
        assertTrue(indexer.hasKey("color\0blue")); // Fourth pair
    }

    @Test
    void testFetchBatch() throws Exception {
        List<DependencyEntry> batch = indexer.fetchBatch(0);
        
        // Should get all valid hypernym relations
        assertEquals(4, batch.size());
        
        // Verify first entry
        DependencyEntry first = batch.get(0);
        assertEquals(1, first.getDocumentId());
        assertEquals(1, first.getSentenceId());
        assertEquals(0, first.getBeginChar());
        assertEquals(20, first.getEndChar());
        assertEquals("animal", first.getHeadToken());
        assertEquals("cat", first.getDependentToken());
        assertEquals("nmod:such_as", first.getRelation());
        assertEquals(LocalDate.parse("2024-01-15"), first.getTimestamp());
    }

    @Test
    void testProcessPartition() throws Exception {
        // Create test entries
        List<DependencyEntry> batch = indexer.fetchBatch(0);
        var result = indexer.processPartition(batch);
        
        // Should have four unique hypernym-hyponym pairs
        assertEquals(4, result.keySet().size());
        
        // Verify positions are correctly stored
        assertTrue(result.containsKey("animal\0cat"));
        assertTrue(result.containsKey("animal\0dog"));
        assertTrue(result.containsKey("color\0red"));
        assertTrue(result.containsKey("color\0blue"));
        
        // Check position details for first pair
        List<PositionList> positions = result.get("animal\0cat");
        assertEquals(1, positions.size());
        Position pos = positions.get(0).getPositions().get(0);
        assertEquals(1, pos.getDocumentId());
        assertEquals(1, pos.getSentenceId());
        assertEquals(0, pos.getBeginPosition());
        assertEquals(20, pos.getEndPosition());
    }
}

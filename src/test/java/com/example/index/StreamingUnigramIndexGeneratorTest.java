package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;

@ExtendWith(MockitoExtension.class)
class StreamingUnigramIndexGeneratorTest extends BaseIndexTest {
    private static final Logger logger = LoggerFactory.getLogger(StreamingUnigramIndexGeneratorTest.class);
    private Path levelDbPath;
    private Path stopwordsPath;
    private StreamingUnigramIndexGenerator generator;

    @Mock
    private ProgressTracker progress;

    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private static final int LARGE_BATCH_SIZE = 10_000;
    private static final Random random = new Random(42); // Fixed seed for reproducibility

    @BeforeEach
    @Override
    void setUp() throws Exception {
        // Call parent setup to create database
        super.setUp();

        // Create paths
        levelDbPath = tempDir.resolve("leveldb");
        stopwordsPath = tempDir.resolve("stopwords.txt");

        // Create empty stopwords file
        Files.writeString(stopwordsPath, "the\na\nan\n");

        // Create generator
        generator = new StreamingUnigramIndexGenerator(
            levelDbPath.toString(),
            stopwordsPath.toString(),
            sqliteConn,
            progress
        );
    }

    private void insertBasicTestData() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-03-20')");
            stmt.execute("INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                        "VALUES (1, 1, 0, 4, 'test', 'test', 'NN')");
            stmt.execute("INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                        "VALUES (1, 1, 5, 9, 'word', 'word', 'NN')");
        }
    }

    private void clearDatabase() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("DELETE FROM annotations");
            stmt.execute("DELETE FROM documents");
        }
    }

    @Test
    void testFetchBatch() throws SQLException {
        // Insert test data
        insertBasicTestData();

        // Fetch batch
        List<AnnotationEntry> entries = generator.fetchBatch(0);

        // Verify results
        assertEquals(2, entries.size());
        
        AnnotationEntry first = entries.get(0);
        assertEquals(1, first.getDocumentId());
        assertEquals(1, first.getSentenceId());
        assertEquals(0, first.getBeginChar());
        assertEquals(4, first.getEndChar());
        assertEquals("test", first.getLemma());
        assertEquals("NN", first.getPos());
        assertEquals(LocalDate.parse("2024-03-20"), first.getTimestamp());

        AnnotationEntry second = entries.get(1);
        assertEquals(1, second.getDocumentId());
        assertEquals(1, second.getSentenceId());
        assertEquals(5, second.getBeginChar());
        assertEquals(9, second.getEndChar());
        assertEquals("word", second.getLemma());
        assertEquals("NN", second.getPos());
        assertEquals(LocalDate.parse("2024-03-20"), second.getTimestamp());
    }

    @Test
    void testProcessBatch() throws IOException {
        // Create test entries
        List<AnnotationEntry> batch = List.of(
            new AnnotationEntry(1, 1, 0, 4, "Test", "NN", LocalDate.parse("2024-03-20")),
            new AnnotationEntry(1, 1, 5, 9, "word", "NN", LocalDate.parse("2024-03-20")),
            new AnnotationEntry(1, 1, 10, 13, "the", "DT", LocalDate.parse("2024-03-20")) // stopword
        );

        // Process batch
        var result = generator.processBatch(batch);

        // Verify results
        assertEquals(2, result.keySet().size());
        assertTrue(result.containsKey("test"));
        assertTrue(result.containsKey("word"));
        assertFalse(result.containsKey("the")); // stopword should be filtered

        // Verify positions
        var testPositions = result.get("test").get(0);
        assertEquals(1, testPositions.getPositions().size());
        assertEquals(0, testPositions.getPositions().get(0).getBeginPosition());
        assertEquals(4, testPositions.getPositions().get(0).getEndPosition());

        var wordPositions = result.get("word").get(0);
        assertEquals(1, wordPositions.getPositions().size());
        assertEquals(5, wordPositions.getPositions().get(0).getBeginPosition());
        assertEquals(9, wordPositions.getPositions().get(0).getEndPosition());
    }

    @Test
    void testPositionListMergingWithOverlaps() throws IOException {
        // Create test entries with overlapping positions
        List<AnnotationEntry> batch = List.of(
            // Overlapping positions for "test"
            new AnnotationEntry(1, 1, 0, 4, "test", "NN", LocalDate.parse("2024-03-20")),
            new AnnotationEntry(1, 1, 2, 6, "test", "NN", LocalDate.parse("2024-03-20")),
            // Adjacent positions for "word"
            new AnnotationEntry(1, 1, 10, 14, "word", "NN", LocalDate.parse("2024-03-20")),
            new AnnotationEntry(1, 1, 15, 19, "word", "NN", LocalDate.parse("2024-03-20")),
            // Exact same position for "repeat"
            new AnnotationEntry(1, 1, 20, 26, "repeat", "NN", LocalDate.parse("2024-03-20")),
            new AnnotationEntry(1, 1, 20, 26, "repeat", "NN", LocalDate.parse("2024-03-20"))
        );

        // Process batch
        var result = generator.processBatch(batch);

        // Verify overlapping positions for "test"
        var testPositions = result.get("test").get(0);
        assertEquals(2, testPositions.getPositions().size(), "Expected two distinct positions for overlapping 'test' entries");
        assertEquals(0, testPositions.getPositions().get(0).getBeginPosition());
        assertEquals(4, testPositions.getPositions().get(0).getEndPosition());
        assertEquals(2, testPositions.getPositions().get(1).getBeginPosition());
        assertEquals(6, testPositions.getPositions().get(1).getEndPosition());

        // Verify adjacent positions for "word"
        var wordPositions = result.get("word").get(0);
        assertEquals(2, wordPositions.getPositions().size(), "Expected two positions for adjacent 'word' entries");
        assertEquals(10, wordPositions.getPositions().get(0).getBeginPosition());
        assertEquals(14, wordPositions.getPositions().get(0).getEndPosition());
        assertEquals(15, wordPositions.getPositions().get(1).getBeginPosition());
        assertEquals(19, wordPositions.getPositions().get(1).getEndPosition());

        // Verify deduplication of exact matches for "repeat"
        var repeatPositions = result.get("repeat").get(0);
        assertEquals(2, repeatPositions.getPositions().size(), "Expected two positions for 'repeat' as they are separate entries");
        assertEquals(20, repeatPositions.getPositions().get(0).getBeginPosition());
        assertEquals(26, repeatPositions.getPositions().get(0).getEndPosition());
        assertEquals(20, repeatPositions.getPositions().get(1).getBeginPosition());
        assertEquals(26, repeatPositions.getPositions().get(1).getEndPosition());
    }

    @Test
    void testMemoryBounds() throws Exception {
        // Clear database
        clearDatabase();

        // Record initial memory
        System.gc(); // Request GC to get more accurate baseline
        long initialMemory = memoryBean.getHeapMemoryUsage().getUsed();
        
        // Insert large batch of test data
        insertLargeTestDataset(LARGE_BATCH_SIZE);

        // Generate index
        generator.generateIndex();

        // Record peak memory
        System.gc(); // Request GC to get more accurate measurement
        long peakMemory = memoryBean.getHeapMemoryUsage().getUsed();

        // Verify memory growth is bounded
        // We expect memory growth to be less than 1GB regardless of input size
        long memoryGrowth = peakMemory - initialMemory;
        assertTrue(memoryGrowth < 1024 * 1024 * 1024L, 
            "Memory growth (" + memoryGrowth / 1024 / 1024 + "MB) exceeded bounds");
    }

    @Test
    void testLargeDatasetProcessing() throws Exception {
        // Clear database
        clearDatabase();

        // Insert large dataset
        int numDocuments = 100;
        int entriesPerDocument = 1000;
        insertLargeTestDataset(numDocuments * entriesPerDocument);

        // Generate index
        long startTime = System.nanoTime();
        generator.generateIndex();
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;

        // Verify progress tracking
        verify(progress, atLeast(numDocuments)).updateIndex(anyLong());

        // Log performance metrics
        logger.info("Processed {} entries in {} ms ({} entries/sec)", 
            numDocuments * entriesPerDocument,
            durationMs,
            (numDocuments * entriesPerDocument * 1000L) / durationMs);
    }

    private void insertLargeTestDataset(int numEntries) throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Create documents
            int currentDoc = 1;
            int entriesInCurrentDoc = 0;
            
            stmt.execute("BEGIN TRANSACTION");
            
            // Insert documents and annotations
            for (int i = 0; i < numEntries; i++) {
                // Create new document every 100 entries
                if (entriesInCurrentDoc == 0) {
                    stmt.execute(String.format(
                        "INSERT INTO documents (document_id, timestamp) VALUES (%d, '2024-03-20')",
                        currentDoc));
                }

                // Generate random word (3-10 chars)
                String word = generateRandomWord(3 + random.nextInt(8));
                int beginChar = entriesInCurrentDoc * 6;
                int endChar = beginChar + word.length();

                // Insert annotation
                stmt.execute(String.format(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) " +
                    "VALUES (%d, %d, %d, %d, '%s', '%s', 'NN')",
                    currentDoc, 1 + entriesInCurrentDoc / 20, beginChar, endChar, word, word));

                entriesInCurrentDoc++;
                if (entriesInCurrentDoc == 100) {
                    currentDoc++;
                    entriesInCurrentDoc = 0;
                }
            }
            
            stmt.execute("COMMIT");
        }
    }

    private String generateRandomWord(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
} 

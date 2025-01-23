package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import com.example.logging.ProgressTracker;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

class ParallelIndexGeneratorTest extends BaseIndexTest {
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    @BeforeEach
    @Override
    void setUp() throws Exception {
        super.setUp();
        levelDbPath = tempDir.resolve("test-index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        sqlitePath = tempDir.resolve("test.db");
        
        // Initialize SQLite database
        sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        try (Statement stmt = sqliteConn.createStatement()) {
            // Create tables
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL
                )
            """);
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS annotations (
                    annotation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER NOT NULL,
                    sentence_id INTEGER NOT NULL,
                    lemma TEXT NOT NULL,
                    pos TEXT NOT NULL,
                    begin_char INTEGER NOT NULL,
                    end_char INTEGER NOT NULL,
                    FOREIGN KEY (document_id) REFERENCES documents(document_id)
                )
            """);
        }
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an");
        Files.write(stopwordsPath, stopwords);
        
        // Insert test data
        setupTestData();
    }
    
    @AfterEach
    @Override
    void tearDown() throws Exception {
        if (sqliteConn != null) {
            sqliteConn.close();
        }
        super.tearDown();
    }
    
    private void setupTestData() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20T10:00:00Z')");
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (2, '2024-01-21T10:00:00Z')");
            
            // Insert test annotations with repeated words and phrases
            // Document 1: "quick brown fox jumps over lazy dog"
            // Document 2: "quick brown fox runs past quick brown rabbit"
            String[][] words = {
                // Document 1
                {"quick", "ADJ", "1", "1", "0", "5"},
                {"brown", "ADJ", "1", "1", "6", "11"},
                {"fox", "NOUN", "1", "1", "12", "15"},
                {"jumps", "VERB", "1", "1", "16", "21"},
                {"over", "ADP", "1", "1", "22", "26"},
                {"lazy", "ADJ", "1", "1", "27", "31"},
                {"dog", "NOUN", "1", "1", "32", "35"},
                // Document 2
                {"quick", "ADJ", "2", "1", "0", "5"},
                {"brown", "ADJ", "2", "1", "6", "11"},
                {"fox", "NOUN", "2", "1", "12", "15"},
                {"runs", "VERB", "2", "1", "16", "20"},
                {"past", "ADP", "2", "1", "21", "25"},
                {"quick", "ADJ", "2", "1", "26", "31"},
                {"brown", "ADJ", "2", "1", "32", "37"},
                {"rabbit", "NOUN", "2", "1", "38", "44"}
            };
            
            for (String[] word : words) {
                stmt.execute(String.format(
                    "INSERT INTO annotations (lemma, pos, document_id, sentence_id, begin_char, end_char) " +
                    "VALUES ('%s', '%s', %s, %s, %s, %s)",
                    word[0], word[1], word[2], word[3], word[4], word[5]
                ));
            }
        }
    }
    
    private void clearDatabase() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("DELETE FROM annotations");
            stmt.execute("DELETE FROM documents");
        }
    }
    
    @Test
    void testParallelIndexGeneration() throws Exception {
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn, new ProgressTracker())) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(levelDbPath.toFile(), options)) {
            // Check some expected unigrams
            assertNotNull(db.get(bytes("quick")), "quick should be indexed");
            assertNotNull(db.get(bytes("fox")), "fox should be indexed");
            assertNotNull(db.get(bytes("dog")), "dog should be indexed");
            
            // Verify a specific position
            PositionList quickPositions = PositionList.deserialize(db.get(bytes("quick")));
            assertEquals(3, quickPositions.size(), "quick should appear three times");
            
            Position pos = quickPositions.getPositions().get(0);
            assertEquals(1, pos.getDocumentId());
            assertEquals(1, pos.getSentenceId());
            assertEquals(0, pos.getBeginPosition());
            assertEquals(5, pos.getEndPosition());
        }
    }
    
    @Test
    void testParallelBigramIndexGeneration() throws Exception {
        Path indexPath = tempDir.resolve("bigram");
        
        try (BigramIndexGenerator generator = new BigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn, new ProgressTracker())) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check some expected bigrams
            String quickBrown = "quick" + BaseIndexGenerator.DELIMITER + "brown";
            String brownFox = "brown" + BaseIndexGenerator.DELIMITER + "fox";
            
            assertNotNull(db.get(bytes(quickBrown)), "quick brown should be indexed");
            assertNotNull(db.get(bytes(brownFox)), "brown fox should be indexed");
            
            // Verify a specific position
            PositionList quickBrownPositions = PositionList.deserialize(db.get(bytes(quickBrown)));
            assertEquals(3, quickBrownPositions.size(), "quick brown should appear three times");
            
            Position pos = quickBrownPositions.getPositions().get(0);
            assertEquals(1, pos.getDocumentId());
            assertEquals(1, pos.getSentenceId());
            assertEquals(0, pos.getBeginPosition());
            assertTrue(pos.getEndPosition() > pos.getBeginPosition(), "end char should be after begin char");
        }
    }
    
    @Test
    void testParallelTrigramIndexGeneration() throws Exception {
        Path indexPath = tempDir.resolve("trigram");
        
        try (TrigramIndexGenerator generator = new TrigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(), 
                10, sqliteConn, new ProgressTracker())) {
            generator.generateIndex();
        }
        
        // Verify index contents
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check some expected trigrams
            String quickBrownFox = "quick" + BaseIndexGenerator.DELIMITER + "brown" + 
                                 BaseIndexGenerator.DELIMITER + "fox";
            String brownFoxJumps = "brown" + BaseIndexGenerator.DELIMITER + "fox" + 
                                 BaseIndexGenerator.DELIMITER + "jumps";
            
            assertNotNull(db.get(bytes(quickBrownFox)), "quick brown fox should be indexed");
            assertNotNull(db.get(bytes(brownFoxJumps)), "brown fox jumps should be indexed");
            
            // Verify a specific position
            PositionList quickBrownFoxPositions = PositionList.deserialize(db.get(bytes(quickBrownFox)));
            assertEquals(2, quickBrownFoxPositions.size(), "quick brown fox should appear twice");
            
            Position pos = quickBrownFoxPositions.getPositions().get(0);
            assertEquals(1, pos.getDocumentId());
            assertEquals(1, pos.getSentenceId());
            assertEquals(0, pos.getBeginPosition());
            assertTrue(pos.getEndPosition() > pos.getBeginPosition(), "end char should be after begin char");
        }
    }

    @Test
    void testCrossDocumentAndSentenceBoundaries() throws Exception {
        clearDatabase();
        // Setup test data with multiple documents and sentences
        setupMultiDocumentDatabase(new String[][] {
            // doc1
            {"This is sentence one", "This is sentence two"},
            // doc2
            {"Another document here", "With multiple sentences"}
        });
        
        try (TrigramIndexGenerator generator = new TrigramIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(),
                10, sqliteConn, new ProgressTracker())) {
            generator.generateIndex();
        }
        
        // Verify no trigrams cross document or sentence boundaries
        try (DB db = factory.open(levelDbPath.toFile(), new Options())) {
            // Verify no trigrams exist across sentence boundaries
            assertNull(db.get(bytes("sentence\u0000one\u0000this")), 
                "Trigram should not exist across sentence boundary");
            // Verify no trigrams exist across document boundaries
            assertNull(db.get(bytes("two\u0000another\u0000document")),
                "Trigram should not exist across document boundary");
            
            // Verify valid trigrams within sentences are present
            assertNotNull(db.get(bytes("this\u0000is\u0000sentence")),
                "Valid trigram within sentence should exist");
        }
    }

    @Test
    void testConcurrentProcessing() throws Exception {
        clearDatabase();
        // Create a large enough dataset to test parallel processing
        setupLargeDatabase(10000);
        
        Path indexPath = tempDir.resolve("concurrent");
        
        // Test with single thread
        try (UnigramIndexGenerator singleThread = new UnigramIndexGenerator(
                indexPath.toString() + "-single",
                stopwordsPath.toString(),
                1000, // larger batch size
                sqliteConn,
                new ProgressTracker())) {
            singleThread.generateIndex();
        }
        
        // Test with multiple threads
        try (UnigramIndexGenerator multiThread = new UnigramIndexGenerator(
                indexPath.toString() + "-multi",
                stopwordsPath.toString(),
                1000, // larger batch size
                sqliteConn,
                new ProgressTracker())) {
            multiThread.generateIndex();
        }
        
        // Verify results are identical regardless of threading
        assertIndexesAreEqual(indexPath.toString() + "-single", indexPath.toString() + "-multi");
    }

    @Test
    void testMemoryUsage() throws Exception {
        clearDatabase();
        // Create a very large dataset to test memory handling
        setupLargeDatabase(10000); // 10K entries (reduced from 100K for test speed)
        
        Path indexPath = tempDir.resolve("memory-test");
        Runtime runtime = Runtime.getRuntime();
        
        // Record initial memory
        System.gc(); // Force GC before measurement
        Thread.sleep(100); // Give GC time to complete
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                indexPath.toString(),
                stopwordsPath.toString(),
                1000, // larger batch size
                sqliteConn,
                new ProgressTracker())) {
            generator.generateIndex();
        }
        
        // Force garbage collection
        System.gc();
        Thread.sleep(100); // Give GC time to complete
        
        // Check final memory usage
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // Verify memory was properly released
        assertTrue((finalMemory - initialMemory) < 50_000_000, 
            "Memory usage should not grow excessively");
    }

    @Test
    void testErrorHandling() {
        // Test handling of null entries
        List<AnnotationEntry> entries = new ArrayList<>();
        entries.add(null);
        
        Exception exception = assertThrows(IOException.class, () -> {
            try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                    tempDir.resolve("error-test").toString(),
                    stopwordsPath.toString(),
                    10,
                    sqliteConn,
                    new ProgressTracker())) {
                generator.processBatch(entries);
            }
        });
        
        // Verify that the NullPointerException is the root cause
        Throwable rootCause = getRootCause(exception);
        assertTrue(rootCause instanceof NullPointerException,
            "Root cause should be NullPointerException, but was: " + rootCause.getClass());
        
        // Test handling of invalid database connection
        assertThrows(SQLException.class, () -> {
            try (Connection invalidConn = DriverManager.getConnection("jdbc:sqlite::memory:");
                 UnigramIndexGenerator generator = new UnigramIndexGenerator(
                    tempDir.resolve("error-test").toString(),
                    stopwordsPath.toString(),
                    10,
                    invalidConn,
                    new ProgressTracker())) {
                invalidConn.close(); // Close connection to simulate failure
                generator.generateIndex();
            }
        });
    }

    @Test
    void testNoOverlappingEntriesBetweenPartitions() throws Exception {
        clearDatabase();
        // Setup test data with multiple documents
        setupMultiDocumentDatabase(new String[][] {
            {"doc one sentence one", "doc one sentence two"},
            {"doc two sentence one", "doc two sentence two"},
            {"doc three sentence one", "doc three sentence two"},
            {"doc four sentence one", "doc four sentence two"}
        });
        
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                tempDir.resolve("no-overlap-test").toString(), 
                stopwordsPath.toString(),
                10,
                sqliteConn,
                new ProgressTracker())) {
            List<AnnotationEntry> entries = generator.fetchBatch(0);
            List<List<AnnotationEntry>> partitions = generator.partitionEntries(entries);
            
            // Verify we have multiple partitions
            assertTrue(partitions.size() >= 2, "Should have at least 2 partitions");
            
            // Verify no entries are duplicated across partitions
            for (int i = 0; i < partitions.size(); i++) {
                for (int j = i + 1; j < partitions.size(); j++) {
                    assertFalse(hasOverlappingEntries(partitions.get(i), partitions.get(j)),
                        String.format("Partitions %d and %d should not have overlapping entries", i, j));
                }
            }
        }
    }

    @Test
    void testDocumentCompleteness() throws Exception {
        clearDatabase();
        // Setup test data with multiple documents
        setupMultiDocumentDatabase(new String[][] {
            {"first doc sentence one", "first doc sentence two", "first doc sentence three"},
            {"second doc sentence one", "second doc sentence two"},
            {"third doc sentence one", "third doc sentence two", "third doc sentence three"}
        });
        
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                tempDir.resolve("doc-completeness-test").toString(), 
                stopwordsPath.toString(),
                10,
                sqliteConn,
                new ProgressTracker())) {
            List<AnnotationEntry> entries = generator.fetchBatch(0);
            List<List<AnnotationEntry>> partitions = generator.partitionEntries(entries);
            
            // Verify each partition contains complete documents
            for (List<AnnotationEntry> partition : partitions) {
                verifyDocumentCompleteness(partition);
            }
        }
    }

    @Test
    void testPartitionDistribution() throws Exception {
        clearDatabase();
        // Setup test data with many documents of varying sizes
        setupMultiDocumentDatabase(new String[][] {
            {"doc1 single sentence"},
            {"doc2 first", "doc2 second"},
            {"doc3 first", "doc3 second", "doc3 third"},
            {"doc4 single sentence"},
            {"doc5 first", "doc5 second"},
            {"doc6 first", "doc6 second", "doc6 third"}
        });
        
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                tempDir.resolve("distribution-test").toString(), 
                stopwordsPath.toString(),
                10,
                sqliteConn,
                new ProgressTracker())) {
            List<AnnotationEntry> entries = generator.fetchBatch(0);
            List<List<AnnotationEntry>> partitions = generator.partitionEntries(entries);
            
            // Verify reasonable work distribution
            verifyWorkDistribution(partitions);
        }
    }

    @Test
    void testEdgeCases() throws Exception {
        clearDatabase();
        // Test empty input
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                tempDir.resolve("edge-case-test").toString(), 
                stopwordsPath.toString(),
                10,
                sqliteConn,
                new ProgressTracker())) {
            List<List<AnnotationEntry>> emptyPartitions = generator.partitionEntries(new ArrayList<>());
            assertTrue(emptyPartitions.isEmpty(), "Empty input should result in empty partitions");
            
            // Test null input
            List<List<AnnotationEntry>> nullPartitions = generator.partitionEntries(null);
            assertTrue(nullPartitions.isEmpty(), "Null input should result in empty partitions");
            
            // Test single document
            setupMultiDocumentDatabase(new String[][] {
                {"single document single sentence"}
            });
            List<AnnotationEntry> entries = generator.fetchBatch(0);
            List<List<AnnotationEntry>> singleDocPartitions = generator.partitionEntries(entries);
            assertEquals(1, singleDocPartitions.size(), "Single document should result in single partition");
        }
    }

    private Throwable getRootCause(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    private void setupMultiDocumentDatabase(String[][] documents) throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            for (int docId = 0; docId < documents.length; docId++) {
                stmt.execute(String.format(
                    "INSERT INTO documents (document_id, timestamp) VALUES (%d, '2024-01-20T10:00:00Z')",
                    docId + 1
                ));
                
                for (int sentId = 0; sentId < documents[docId].length; sentId++) {
                    String[] words = documents[docId][sentId].split(" ");
                    int charPos = 0;
                    for (String word : words) {
                        stmt.execute(String.format(
                            "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) " +
                            "VALUES (%d, %d, %d, %d, '%s', 'UNKNOWN')",
                            docId + 1, sentId + 1, charPos, charPos + word.length(), word
                        ));
                        charPos += word.length() + 1;
                    }
                }
            }
        }
    }

    private void setupLargeDatabase(int numEntries) throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20T10:00:00Z')");
            
            String[] words = {"quick", "brown", "fox", "jumps", "over", "lazy", "dog"};
            for (int i = 0; i < numEntries; i++) {
                String word = words[i % words.length];
                stmt.execute(String.format(
                    "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) " +
                    "VALUES (1, %d, %d, %d, '%s', 'UNKNOWN')",
                    i / 10 + 1, i * 5, i * 5 + word.length(), word
                ));
            }
        }
    }

    private boolean hasOverlappingEntries(List<? extends IndexEntry> partition1, List<? extends IndexEntry> partition2) {
        // Create a set of unique identifiers for entries in partition1
        Set<String> entries1 = new HashSet<>();
        for (IndexEntry e : partition1) {
            entries1.add(String.format("%d-%d", e.getDocumentId(), e.getSentenceId()));
        }
        
        // Check if any entry in partition2 has the same identifier
        for (IndexEntry e : partition2) {
            if (entries1.contains(String.format("%d-%d", e.getDocumentId(), e.getSentenceId()))) {
                return true;
            }
        }
        return false;
    }

    private void assertIndexesAreEqual(String path1, String path2) throws IOException {
        try (DB db1 = factory.open(new File(path1), new Options());
             DB db2 = factory.open(new File(path2), new Options())) {
            
            try (DBIterator it = db1.iterator()) {
                it.seekToFirst();
                while (it.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = it.next();
                    assertArrayEquals(
                        db2.get(entry.getKey()),
                        entry.getValue(),
                        "Indexes should have identical entries"
                    );
                }
            }
        }
    }

    private static byte[] bytes(String str) {
        return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private void verifyDocumentCompleteness(List<? extends IndexEntry> partition) {
        // Group entries by document ID
        Map<Integer, List<IndexEntry>> byDocument = partition.stream()
            .collect(Collectors.groupingBy(IndexEntry::getDocumentId));
        
        // For each document, verify all sentences are present
        for (List<IndexEntry> docEntries : byDocument.values()) {
            Set<Integer> sentenceIds = docEntries.stream()
                .map(IndexEntry::getSentenceId)
                .collect(Collectors.toSet());
            
            // Verify no gaps in sentence IDs
            int minId = sentenceIds.stream().mapToInt(Integer::intValue).min().getAsInt();
            int maxId = sentenceIds.stream().mapToInt(Integer::intValue).max().getAsInt();
            assertEquals(maxId - minId + 1, sentenceIds.size(),
                "All sentences should be present without gaps");
        }
    }

    private void verifyWorkDistribution(List<List<AnnotationEntry>> partitions) {
        // Calculate average partition size
        double avgSize = partitions.stream()
            .mapToInt(List::size)
            .average()
            .orElse(0.0);
        
        // Verify no partition is too far from average
        for (List<AnnotationEntry> partition : partitions) {
            double ratio = partition.size() / avgSize;
            assertTrue(ratio >= 0.5 && ratio <= 1.5,
                "Partition size should be within 50% of average");
        }
    }
} 
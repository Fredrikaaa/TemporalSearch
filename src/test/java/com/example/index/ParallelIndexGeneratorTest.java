package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

class ParallelIndexGeneratorTest {
    private Path tempDir;
    private Path levelDbPath;
    private Path stopwordsPath;
    private Path sqlitePath;
    private Connection sqliteConn;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories and files
        tempDir = Files.createTempDirectory("index-test-");
        levelDbPath = tempDir.resolve("test-index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        sqlitePath = tempDir.resolve("test.db");
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an");
        Files.write(stopwordsPath, stopwords);
        
        // Create and populate SQLite database
        sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
        setupDatabase();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (sqliteConn != null) {
            sqliteConn.close();
        }
        
        // Clean up temporary files
        Files.walk(tempDir)
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try {
                     Files.delete(path);
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             });
    }
    
    private void setupDatabase() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Create tables
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
                    begin_char INTEGER,
                    end_char INTEGER,
                    lemma TEXT,
                    pos TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
        }
    }

    @Test
    void testPartitionBoundaries() throws Exception {
        // Test that n-grams at partition boundaries are correctly handled
        setupMultiDocumentDatabase(new String[][] {
            {"one two three four five six seven eight"}  // Single document, single sentence
        });
        
        // Force small partition size to test boundary handling
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                tempDir.resolve("boundary-test").toString(), 
                stopwordsPath.toString(),
                4, // small batch size
                sqliteConn,
                2  // force two partitions
        )) {
            List<IndexEntry> entries = generator.fetchBatch(0);
            List<List<IndexEntry>> partitions = generator.partitionEntries(entries);
            
            // Verify overlapping entries exist in both partitions
            assertFalse(partitions.get(0).isEmpty(), "First partition should not be empty");
            assertFalse(partitions.get(1).isEmpty(), "Second partition should not be empty");
            
            // Verify that entries at partition boundaries are duplicated
            assertTrue(hasOverlappingEntries(partitions.get(0), partitions.get(1)),
                "Partitions should have overlapping entries");
            
            // Verify all entries within each partition are from the same document/sentence
            verifyPartitionContinuity(partitions.get(0));
            verifyPartitionContinuity(partitions.get(1));
        }
    }

    @Test
    void testCrossDocumentAndSentenceBoundaries() throws Exception {
        // Setup test data with multiple documents and sentences
        setupMultiDocumentDatabase(new String[][] {
            // doc1
            {"This is sentence one", "This is sentence two"},
            // doc2
            {"Another document here", "With multiple sentences"}
        });
        
        Path indexPath = tempDir.resolve("cross-boundary");
        
        try (TrigramIndexGenerator generator = new TrigramIndexGenerator(
                indexPath.toString(), stopwordsPath.toString(),
                10, sqliteConn, 2)) {
            generator.generateIndex();
        }
        
        // Verify no trigrams cross document or sentence boundaries
        try (DB db = factory.open(indexPath.toFile(), new Options())) {
            // Verify no trigrams exist across sentence boundaries
            assertNull(db.get(bytes("sentence one this")), 
                "Trigram should not exist across sentence boundary");
            // Verify no trigrams exist across document boundaries
            assertNull(db.get(bytes("two another document")),
                "Trigram should not exist across document boundary");
            
            // Verify valid trigrams within sentences are present
            assertNotNull(db.get(bytes("this is sentence")),
                "Valid trigram within sentence should exist");
        }
    }

    @Test
    void testConcurrentProcessing() throws Exception {
        // Create a large enough dataset to test parallel processing
        setupLargeDatabase(100000); // 100K entries for better parallel performance measurement
        
        Path indexPath = tempDir.resolve("concurrent");
        
        // Warm up JVM
        try (UnigramIndexGenerator warmup = new UnigramIndexGenerator(
                indexPath.toString() + "-warmup",
                stopwordsPath.toString(),
                1000,
                sqliteConn,
                1)) {
            warmup.generateIndex();
        }
        
        // Test with single thread
        long startTime = System.currentTimeMillis();
        try (UnigramIndexGenerator singleThread = new UnigramIndexGenerator(
                indexPath.toString() + "-single",
                stopwordsPath.toString(),
                1000, // larger batch size
                sqliteConn,
                1)) {
            singleThread.generateIndex();
        }
        long singleThreadTime = System.currentTimeMillis() - startTime;
        
        // Test with multiple threads
        startTime = System.currentTimeMillis();
        try (UnigramIndexGenerator multiThread = new UnigramIndexGenerator(
                indexPath.toString() + "-multi",
                stopwordsPath.toString(),
                1000, // larger batch size
                sqliteConn,
                Runtime.getRuntime().availableProcessors())) {
            multiThread.generateIndex();
        }
        long multiThreadTime = System.currentTimeMillis() - startTime;
        
        // Verify results are identical
        assertIndexesAreEqual(indexPath.toString() + "-single", indexPath.toString() + "-multi");
        
        // Verify parallel processing is faster
        assertTrue(multiThreadTime < singleThreadTime, 
            String.format("Parallel processing should be faster (single: %dms, multi: %dms)", 
                singleThreadTime, multiThreadTime));
    }

    @Test
    void testMemoryUsage() throws Exception {
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
                4)) {
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
        List<IndexEntry> entries = new ArrayList<>();
        entries.add(null);
        
        Exception exception = assertThrows(IOException.class, () -> {
            try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                    tempDir.resolve("error-test").toString(),
                    stopwordsPath.toString(),
                    10,
                    sqliteConn,
                    2)) {
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
                    2)) {
                invalidConn.close(); // Close connection to simulate failure
                generator.generateIndex();
            }
        });
    }

    private Throwable getRootCause(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
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

    private boolean hasOverlappingEntries(List<IndexEntry> partition1, List<IndexEntry> partition2) {
        Set<String> entries1 = partition1.stream()
            .map(e -> String.format("%d-%d-%s", e.documentId, e.sentenceId, e.lemma))
            .collect(Collectors.toSet());
        
        return partition2.stream()
            .map(e -> String.format("%d-%d-%s", e.documentId, e.sentenceId, e.lemma))
            .anyMatch(entries1::contains);
    }

    private void verifyPartitionContinuity(List<IndexEntry> partition) {
        for (int i = 1; i < partition.size(); i++) {
            IndexEntry prev = partition.get(i - 1);
            IndexEntry curr = partition.get(i);
            if (prev.documentId == curr.documentId && prev.sentenceId != curr.sentenceId) {
                fail("Found entries from different sentences within same document in partition");
            }
        }
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
} 
package com.example.core;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;

/**
 * Tests for IndexAccess implementation.
 * Verifies core functionality including:
 * - Basic CRUD operations
 * - Batch operations
 * - Resource management
 * - Error handling
 */
public class IndexAccessTest {
    private static final String TEST_INDEX_PATH = "test-indexes";
    private IndexAccess indexAccess;
    private Path indexPath;

    @BeforeEach
    void setUp() throws Exception {
        indexPath = Path.of(TEST_INDEX_PATH);
        if (Files.exists(indexPath)) {
            deleteDirectory(indexPath.toFile());
        }
        Files.createDirectories(indexPath);

        Options options = new Options();
        options.createIfMissing(true);
        options.compressionType(CompressionType.SNAPPY);
        options.cacheSize(16 * 1024 * 1024); // 16MB cache

        indexAccess = new IndexAccess(indexPath, "test", options);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (indexAccess != null) {
            indexAccess.close();
        }
        if (Files.exists(indexPath)) {
            deleteDirectory(indexPath.toFile());
        }
    }

    @Test
    void testBasicOperations() throws Exception {
        // Create test data
        Position pos1 = new Position(1, 1, 0, 5, LocalDate.of(2024, 1, 1));
        Position pos2 = new Position(1, 2, 6, 10, LocalDate.of(2024, 1, 1));
        PositionList positions = new PositionList();
        positions.add(pos1);
        positions.add(pos2);

        // Test put and get
        byte[] key = "test-key".getBytes();
        indexAccess.put(key, positions);

        Optional<PositionList> retrieved = indexAccess.get(key);
        assertTrue(retrieved.isPresent(), "Should retrieve stored positions");
        assertEquals(2, retrieved.get().getPositions().size(), "Should have correct number of positions");

        // Verify position details
        Position retrievedPos = retrieved.get().getPositions().get(0);
        assertEquals(pos1.getDocumentId(), retrievedPos.getDocumentId());
        assertEquals(pos1.getSentenceId(), retrievedPos.getSentenceId());
        assertEquals(pos1.getBeginPosition(), retrievedPos.getBeginPosition());
        assertEquals(pos1.getEndPosition(), retrievedPos.getEndPosition());
        assertEquals(pos1.getTimestamp(), retrievedPos.getTimestamp());
    }

    @Test
    void testBatchOperations() throws Exception {
        // Create test data
        Map<String, PositionList> entries = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Position pos = new Position(i, 1, 0, 5, LocalDate.now());
            PositionList positions = new PositionList();
            positions.add(pos);
            entries.put("key" + i, positions);
        }

        // Write batch
        WriteBatch batch = indexAccess.createWriteBatch();
        try {
            for (Map.Entry<String, PositionList> entry : entries.entrySet()) {
                batch.put(
                    entry.getKey().getBytes(),
                    entry.getValue().serialize()
                );
            }
            indexAccess.write(batch);
        } finally {
            batch.close();
        }

        // Verify entries
        for (String key : entries.keySet()) {
            Optional<PositionList> retrieved = indexAccess.get(key.getBytes());
            assertTrue(retrieved.isPresent(), "Should retrieve entry for key: " + key);
            assertEquals(1, retrieved.get().getPositions().size(),
                "Should have correct number of positions for key: " + key);
        }
    }

    @Test
    void testMergePositions() throws Exception {
        byte[] key = "merge-test".getBytes();
        
        // Create first position list
        Position pos1 = new Position(1, 1, 0, 5, LocalDate.now());
        PositionList positions1 = new PositionList();
        positions1.add(pos1);
        indexAccess.put(key, positions1);

        // Create second position list
        Position pos2 = new Position(1, 2, 6, 10, LocalDate.now());
        PositionList positions2 = new PositionList();
        positions2.add(pos2);
        indexAccess.put(key, positions2);

        // Verify merge
        Optional<PositionList> merged = indexAccess.get(key);
        assertTrue(merged.isPresent(), "Should retrieve merged positions");
        assertEquals(2, merged.get().getPositions().size(), "Should contain all positions");
    }

    @Test
    void testIterator() throws Exception {
        // Create test data
        for (int i = 0; i < 5; i++) {
            Position pos = new Position(i, 1, 0, 5, LocalDate.now());
            PositionList positions = new PositionList();
            positions.add(pos);
            indexAccess.put(("key" + i).getBytes(), positions);
        }

        // Test iteration
        int count = 0;
        try (DBIterator iterator = indexAccess.iterator()) {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                Map.Entry<byte[], byte[]> entry = iterator.peekNext();
                assertNotNull(entry.getKey(), "Key should not be null");
                assertNotNull(entry.getValue(), "Value should not be null");
                
                PositionList positions = PositionList.deserialize(entry.getValue());
                assertEquals(1, positions.getPositions().size(),
                    "Should have correct number of positions");
                count++;
            }
        }
        assertEquals(5, count, "Should iterate over all entries");
    }

    @Test
    void testClosedOperations() throws Exception {
        indexAccess.close();

        // Verify operations throw appropriate exceptions
        assertThrows(IndexAccessException.class, () -> 
            indexAccess.put("test".getBytes(), new PositionList()));
        assertThrows(IndexAccessException.class, () -> 
            indexAccess.get("test".getBytes()));
        assertThrows(IndexAccessException.class, () -> 
            indexAccess.write(indexAccess.createWriteBatch()));
        assertThrows(IndexAccessException.class, () -> 
            indexAccess.iterator());
    }

    private void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        if (!file.delete()) {
                            throw new IOException("Failed to delete file: " + file);
                        }
                    }
                }
            }
            if (!directory.delete()) {
                throw new IOException("Failed to delete directory: " + directory);
            }
        }
    }
} 
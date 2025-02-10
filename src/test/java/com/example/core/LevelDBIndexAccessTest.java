package com.example.core;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

/**
 * Tests for LevelDBIndexAccess implementation.
 * Verifies core functionality including:
 * - Position storage and retrieval
 * - Metadata management
 * - Temporal range tracking
 */
public class LevelDBIndexAccessTest {
    private static final String TEST_INDEX_PATH = "test-index";
    private LevelDBIndexAccess indexAccess;
    private Path indexPath;

    @BeforeEach
    void setUp() throws IOException {
        indexPath = Path.of(TEST_INDEX_PATH);
        if (Files.exists(indexPath)) {
            deleteDirectory(indexPath.toFile());
        }
        indexAccess = new LevelDBIndexAccess(TEST_INDEX_PATH);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (indexAccess != null) {
            indexAccess.close();
        }
        if (Files.exists(indexPath)) {
            deleteDirectory(indexPath.toFile());
        }
    }

    private void deleteDirectory(File directory) throws IOException {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    Files.delete(file.toPath());
                }
            }
        }
        Files.delete(directory.toPath());
    }

    @Test
    void testBasicPositionStorage() throws IOException {
        // Create test position
        Position pos = new Position(1, 1, 0, 5, LocalDate.of(2024, 1, 1));
        
        // Add position
        indexAccess.addPosition("test", pos);
        
        // Retrieve and verify
        List<Position> positions = indexAccess.getPositions("test");
        assertEquals(1, positions.size());
        assertEquals(pos, positions.get(0));
    }

    @Test
    void testMultiplePositions() throws IOException {
        // Create test positions
        Position pos1 = new Position(1, 1, 0, 5, LocalDate.of(2024, 1, 1));
        Position pos2 = new Position(1, 2, 10, 15, LocalDate.of(2024, 1, 2));
        
        // Add positions
        indexAccess.addPosition("test", pos1);
        indexAccess.addPosition("test", pos2);
        
        // Retrieve and verify
        List<Position> positions = indexAccess.getPositions("test");
        assertEquals(2, positions.size());
        assertTrue(positions.contains(pos1));
        assertTrue(positions.contains(pos2));
    }

    @Test
    void testTimeRangeTracking() throws IOException {
        // Add positions with different dates
        Position pos1 = new Position(1, 1, 0, 5, LocalDate.of(2024, 1, 1));
        Position pos2 = new Position(1, 2, 10, 15, LocalDate.of(2024, 12, 31));
        
        indexAccess.addPosition("test1", pos1);
        indexAccess.addPosition("test2", pos2);
        
        // Verify time range
        LocalDate[] range = indexAccess.getTimeRange();
        assertNotNull(range);
        assertEquals(LocalDate.of(2024, 1, 1), range[0]);
        assertEquals(LocalDate.of(2024, 12, 31), range[1]);
    }

    @Test
    void testEntryCount() throws IOException {
        assertEquals(0, indexAccess.getEntryCount());
        
        // Add positions to different keys
        Position pos = new Position(1, 1, 0, 5, LocalDate.of(2024, 1, 1));
        indexAccess.addPosition("test1", pos);
        assertEquals(1, indexAccess.getEntryCount());
        
        indexAccess.addPosition("test2", pos);
        assertEquals(2, indexAccess.getEntryCount());
        
        // Adding to existing key shouldn't increase count
        indexAccess.addPosition("test1", pos);
        assertEquals(2, indexAccess.getEntryCount());
    }

    @Test
    void testContainsKey() throws IOException {
        Position pos = new Position(1, 1, 0, 5, LocalDate.of(2024, 1, 1));
        
        assertFalse(indexAccess.containsKey("test"));
        indexAccess.addPosition("test", pos);
        assertTrue(indexAccess.containsKey("test"));
    }

    @Test
    void testEmptyPositions() throws IOException {
        List<Position> positions = indexAccess.getPositions("nonexistent");
        assertTrue(positions.isEmpty());
    }

    @Test
    void testEmptyTimeRange() throws IOException {
        LocalDate[] range = indexAccess.getTimeRange();
        assertNull(range);
    }
} 
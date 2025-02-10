package com.example.core;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.List;

/**
 * Tests for LevelDBIndexAccess implementation.
 * Verifies core functionality like position storage, retrieval,
 * and metadata management.
 */
public class LevelDBIndexAccessTest {
    private static final String TEST_INDEX_PATH = "test_index";
    private LevelDBIndexAccess indexAccess;
    
    @BeforeEach
    void setUp() throws Exception {
        // Clean up any existing test data
        Path indexPath = Paths.get(TEST_INDEX_PATH);
        if (Files.exists(indexPath)) {
            Files.walk(indexPath)
                 .sorted((a, b) -> b.compareTo(a))
                 .forEach(path -> {
                     try {
                         Files.deleteIfExists(path);
                     } catch (Exception e) {
                         // Ignore cleanup errors
                     }
                 });
        }
        
        indexAccess = new LevelDBIndexAccess(TEST_INDEX_PATH);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (indexAccess != null) {
            indexAccess.close();
        }
        
        // Clean up test data
        Path indexPath = Paths.get(TEST_INDEX_PATH);
        if (Files.exists(indexPath)) {
            Files.walk(indexPath)
                 .sorted((a, b) -> b.compareTo(a))
                 .forEach(path -> {
                     try {
                         Files.deleteIfExists(path);
                     } catch (Exception e) {
                         // Ignore cleanup errors
                     }
                 });
        }
    }
    
    @Test
    void testBasicOperations() throws Exception {
        // Test initial state
        assertEquals(0, indexAccess.getEntryCount());
        assertNull(indexAccess.getTimeRange());
        assertFalse(indexAccess.containsKey("test"));
        
        // Add a position
        LocalDate date = LocalDate.of(2024, 1, 1);
        Position pos = new Position(1, 1, 0, 4, date);
        indexAccess.addPosition("test", pos);
        
        // Verify state after addition
        assertEquals(1, indexAccess.getEntryCount());
        assertTrue(indexAccess.containsKey("test"));
        
        // Verify position retrieval
        List<Position> positions = indexAccess.getPositions("test");
        assertEquals(1, positions.size());
        Position retrieved = positions.get(0);
        assertEquals(1, retrieved.getDocumentId());
        assertEquals(1, retrieved.getSentenceId());
        assertEquals(0, retrieved.getBeginPosition());
        assertEquals(4, retrieved.getEndPosition());
        assertEquals(date, retrieved.getTimestamp());
        
        // Verify time range
        LocalDate[] timeRange = indexAccess.getTimeRange();
        assertNotNull(timeRange);
        assertEquals(date, timeRange[0]);
        assertEquals(date, timeRange[1]);
    }
    
    @Test
    void testMultiplePositions() throws Exception {
        // Add multiple positions for same key
        LocalDate date1 = LocalDate.of(2024, 1, 1);
        LocalDate date2 = LocalDate.of(2024, 1, 2);
        
        Position pos1 = new Position(1, 1, 0, 4, date1);
        Position pos2 = new Position(1, 2, 5, 9, date2);
        
        indexAccess.addPosition("test", pos1);
        indexAccess.addPosition("test", pos2);
        
        // Verify positions
        List<Position> positions = indexAccess.getPositions("test");
        assertEquals(2, positions.size());
        
        // Verify time range spans both dates
        LocalDate[] timeRange = indexAccess.getTimeRange();
        assertNotNull(timeRange);
        assertEquals(date1, timeRange[0]);
        assertEquals(date2, timeRange[1]);
    }
    
    @Test
    void testNonExistentKey() throws Exception {
        List<Position> positions = indexAccess.getPositions("nonexistent");
        assertTrue(positions.isEmpty());
        assertFalse(indexAccess.containsKey("nonexistent"));
    }
    
    @Test
    void testTemporalRangeUpdates() throws Exception {
        // Add positions with different dates
        LocalDate date1 = LocalDate.of(2024, 1, 1);
        LocalDate date2 = LocalDate.of(2024, 2, 1);
        LocalDate date3 = LocalDate.of(2023, 12, 1);
        
        indexAccess.addPosition("test1", new Position(1, 1, 0, 4, date1));
        indexAccess.addPosition("test2", new Position(2, 1, 0, 4, date2));
        indexAccess.addPosition("test3", new Position(3, 1, 0, 4, date3));
        
        // Verify time range encompasses all dates
        LocalDate[] timeRange = indexAccess.getTimeRange();
        assertNotNull(timeRange);
        assertEquals(date3, timeRange[0]); // Earliest date
        assertEquals(date2, timeRange[1]); // Latest date
    }
} 
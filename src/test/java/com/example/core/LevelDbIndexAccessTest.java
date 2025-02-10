package com.example.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LevelDbIndexAccessTest {
    @TempDir
    Path tempDir;
    
    private LevelDbIndexAccess indexAccess;
    private static final LocalDate TEST_DATE = LocalDate.of(2024, 1, 1);
    
    @BeforeEach
    void setUp() throws IOException {
        String dbPath = tempDir.resolve("test.db").toString();
        indexAccess = new LevelDbIndexAccess(dbPath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (indexAccess != null) {
            indexAccess.close();
        }
    }
    
    @Test
    void testAddAndRetrievePosition() throws IOException {
        // Create test position
        Position position = new Position(1, 1, 0, 10, TEST_DATE);
        
        // Add position
        String key = "test_key";
        indexAccess.addPosition(key, position);
        
        // Retrieve and verify
        List<Position> positions = indexAccess.getPositions(key);
        assertEquals(1, positions.size());
        
        Position retrieved = positions.get(0);
        assertEquals(position.getDocumentId(), retrieved.getDocumentId());
        assertEquals(position.getSentenceId(), retrieved.getSentenceId());
        assertEquals(position.getBeginChar(), retrieved.getBeginChar());
        assertEquals(position.getEndChar(), retrieved.getEndChar());
        assertEquals(position.getTimestamp(), retrieved.getTimestamp());
    }
    
    @Test
    void testMultiplePositions() throws IOException {
        String key = "multi_key";
        Position pos1 = new Position(1, 1, 0, 10, TEST_DATE);
        Position pos2 = new Position(1, 2, 11, 20, TEST_DATE);
        
        indexAccess.addPosition(key, pos1);
        indexAccess.addPosition(key, pos2);
        
        List<Position> positions = indexAccess.getPositions(key);
        assertEquals(2, positions.size());
        assertTrue(positions.contains(pos1));
        assertTrue(positions.contains(pos2));
    }
    
    @Test
    void testContainsKey() throws IOException {
        String key = "exists_key";
        Position position = new Position(1, 1, 0, 10, TEST_DATE);
        
        assertFalse(indexAccess.containsKey(key));
        indexAccess.addPosition(key, position);
        assertTrue(indexAccess.containsKey(key));
    }
    
    @Test
    void testEntryCount() throws IOException {
        assertEquals(0, indexAccess.getEntryCount());
        
        Position position = new Position(1, 1, 0, 10, TEST_DATE);
        indexAccess.addPosition("key1", position);
        assertEquals(1, indexAccess.getEntryCount());
        
        indexAccess.addPosition("key2", position);
        assertEquals(2, indexAccess.getEntryCount());
    }
    
    @Test
    void testTimeRange() throws IOException {
        LocalDate date1 = LocalDate.of(2024, 1, 1);
        LocalDate date2 = LocalDate.of(2024, 1, 2);
        
        Position pos1 = new Position(1, 1, 0, 10, date1);
        Position pos2 = new Position(2, 1, 0, 10, date2);
        
        indexAccess.addPosition("key1", pos1);
        indexAccess.addPosition("key2", pos2);
        
        LocalDate[] range = indexAccess.getTimeRange();
        assertEquals(date1, range[0]);
        assertEquals(date2, range[1]);
    }
    
    @Test
    void testEmptyTimeRange() {
        LocalDate[] range = indexAccess.getTimeRange();
        assertNull(range[0]);
        assertNull(range[1]);
    }
} 
package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class ExternalSortConfigTest {
    
    @Test
    void testDefaultValues() {
        ExternalSortConfig config = new ExternalSortConfig.Builder().build();
        assertEquals(512, config.getMaxMemoryMb());
        assertEquals(32, config.getBufferSizeMb());
        assertEquals(100, config.getMaxFilesPerMerge());
        assertTrue(config.getMergeThreads() > 0 && config.getMergeThreads() <= Runtime.getRuntime().availableProcessors());
    }
    
    @Test
    void testCustomValues() {
        ExternalSortConfig config = new ExternalSortConfig.Builder()
            .withMaxMemoryMb(1024)
            .withBufferSizeMb(64)
            .withMaxFilesPerMerge(200)
            .withMergeThreads(4)
            .build();
            
        assertEquals(1024, config.getMaxMemoryMb());
        assertEquals(64, config.getBufferSizeMb());
        assertEquals(200, config.getMaxFilesPerMerge());
        assertEquals(4, config.getMergeThreads());
    }
    
    @Test
    void testInvalidMaxMemory() {
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMaxMemoryMb(0).build());
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMaxMemoryMb(-1).build());
        assertThrows(IllegalStateException.class, () -> 
            new ExternalSortConfig.Builder().withMaxMemoryMb(32).build());
    }
    
    @Test
    void testInvalidBufferSize() {
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withBufferSizeMb(0).build());
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withBufferSizeMb(-1).build());
        assertThrows(IllegalStateException.class, () -> 
            new ExternalSortConfig.Builder().withBufferSizeMb(2).build());
        
        // Buffer size larger than max memory
        assertThrows(IllegalStateException.class, () -> 
            new ExternalSortConfig.Builder()
                .withMaxMemoryMb(128)
                .withBufferSizeMb(256)
                .build());
    }
    
    @Test
    void testInvalidMaxFilesPerMerge() {
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMaxFilesPerMerge(0).build());
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMaxFilesPerMerge(1).build());
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMaxFilesPerMerge(-1).build());
    }
    
    @Test
    void testInvalidMergeThreads() {
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMergeThreads(0).build());
        assertThrows(IllegalArgumentException.class, () -> 
            new ExternalSortConfig.Builder().withMergeThreads(-1).build());
    }
} 
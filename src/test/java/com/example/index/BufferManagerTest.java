package com.example.index;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class BufferManagerTest {
    
    @Test
    void testBufferSizeCalculation() {
        ExternalSortConfig config = new ExternalSortConfig.Builder()
            .withMaxMemoryMb(512)
            .withBufferSizeMb(32)
            .withMaxFilesPerMerge(100)
            .withMergeThreads(4)
            .build();
            
        BufferManager manager = new BufferManager(config);
        
        // Buffer size should be a power of 2
        int bufferSize = manager.getIndividualBufferSize();
        assertTrue(isPowerOfTwo(bufferSize));
        
        // Buffer size should be at least 4MB
        assertTrue(bufferSize >= 4 * 1024 * 1024);
        
        // Total buffers should fit in max memory
        long totalBufferMemory = (long) manager.getMaxConcurrentBuffers() * bufferSize;
        assertTrue(totalBufferMemory <= 512L * 1024 * 1024);
    }
    
    @Test
    void testMinimumBufferSize() {
        ExternalSortConfig config = new ExternalSortConfig.Builder()
            .withMaxMemoryMb(64)
            .withBufferSizeMb(4)
            .withMaxFilesPerMerge(10)
            .withMergeThreads(2)
            .build();
            
        BufferManager manager = new BufferManager(config);
        
        // Buffer size should be at least 4MB
        assertTrue(manager.getIndividualBufferSize() >= 4 * 1024 * 1024);
        assertTrue(manager.getMaxConcurrentBuffers() >= 1);
    }
    
    @Test
    void testLargeConfiguration() {
        ExternalSortConfig config = new ExternalSortConfig.Builder()
            .withMaxMemoryMb(2048)
            .withBufferSizeMb(128)
            .withMaxFilesPerMerge(200)
            .withMergeThreads(8)
            .build();
            
        BufferManager manager = new BufferManager(config);
        
        int bufferSize = manager.getIndividualBufferSize();
        int maxBuffers = manager.getMaxConcurrentBuffers();
        
        // Verify buffer size is reasonable
        assertTrue(bufferSize >= 4 * 1024 * 1024);
        assertTrue(bufferSize <= 128 * 1024 * 1024);
        
        // Verify total memory usage
        long totalMemory = (long) bufferSize * maxBuffers;
        assertTrue(totalMemory <= 2048L * 1024 * 1024);
        
        // Should have enough buffers for parallel merging
        assertTrue(maxBuffers >= config.getMergeThreads());
    }
    
    private boolean isPowerOfTwo(int n) {
        return n > 0 && (n & (n - 1)) == 0;
    }
} 
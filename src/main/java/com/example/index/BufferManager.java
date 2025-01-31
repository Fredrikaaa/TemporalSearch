package com.example.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages buffer allocation for external sort operations.
 * Optimizes buffer sizes based on available memory and merge configuration.
 */
public class BufferManager {
    private static final Logger logger = LoggerFactory.getLogger(BufferManager.class);
    
    private final int totalBufferSize;
    private final int individualBufferSize;
    private final int maxConcurrentBuffers;
    
    /**
     * Creates a new BufferManager with the given configuration.
     * @param config The external sort configuration
     */
    public BufferManager(ExternalSortConfig config) {
        this.totalBufferSize = config.getMaxMemoryMb() * 1024 * 1024;
        this.individualBufferSize = calculateOptimalBufferSize(config);
        this.maxConcurrentBuffers = calculateMaxConcurrentBuffers(config);
        
        logger.debug("Created BufferManager: totalBuffer={}MB, individualBuffer={}KB, maxConcurrent={}",
            totalBufferSize / (1024 * 1024),
            individualBufferSize / 1024,
            maxConcurrentBuffers);
    }
    
    /**
     * Gets the size of individual buffers for read/write operations.
     * @return The buffer size in bytes
     */
    public int getIndividualBufferSize() {
        return individualBufferSize;
    }
    
    /**
     * Gets the maximum number of concurrent buffers that can be allocated.
     * @return The maximum number of concurrent buffers
     */
    public int getMaxConcurrentBuffers() {
        return maxConcurrentBuffers;
    }
    
    /**
     * Calculates the optimal buffer size based on configuration and system resources.
     * @param config The external sort configuration
     * @return The optimal buffer size in bytes
     */
    private int calculateOptimalBufferSize(ExternalSortConfig config) {
        // Calculate minimum number of buffers needed for merge operations
        // Each merge thread needs (maxFilesPerMerge + 1) buffers
        int minBuffers = config.getMergeThreads() * (config.getMaxFilesPerMerge() + 1);
        
        // Calculate maximum buffer size that allows for minimum number of buffers
        long maxBufferSize = totalBufferSize / minBuffers;
        
        // Start with configured buffer size
        long bufferSize = (long) config.getBufferSizeMb() * 1024 * 1024;
        
        // Ensure buffer size doesn't exceed maximum
        bufferSize = Math.min(bufferSize, maxBufferSize);
        
        // Round down to nearest power of 2 for optimal performance
        bufferSize = roundDownToPowerOf2((int) Math.min(bufferSize, Integer.MAX_VALUE));
        
        // Ensure minimum reasonable size
        bufferSize = Math.max(bufferSize, 4 * 1024 * 1024); // 4MB minimum
        
        return (int) bufferSize;
    }
    
    /**
     * Calculates the maximum number of concurrent buffers that can be allocated.
     * @param config The external sort configuration
     * @return The maximum number of concurrent buffers
     */
    private int calculateMaxConcurrentBuffers(ExternalSortConfig config) {
        // Calculate minimum number of buffers needed for merge operations
        int minBuffers = config.getMergeThreads() * (config.getMaxFilesPerMerge() + 1);
        
        // Calculate actual number of buffers we can allocate
        long maxBuffers = (long) totalBufferSize / individualBufferSize;
        
        // Ensure we don't exceed integer range
        maxBuffers = Math.min(maxBuffers, Integer.MAX_VALUE);
        
        // Ensure we have at least the minimum required buffers
        maxBuffers = Math.max(minBuffers, maxBuffers);
        
        // Ensure we don't exceed total memory
        while (maxBuffers * (long) individualBufferSize > totalBufferSize) {
            maxBuffers--;
        }
        
        // Ensure we have at least enough buffers for merge threads
        maxBuffers = Math.max(maxBuffers, config.getMergeThreads());
        
        return (int) maxBuffers;
    }
    
    /**
     * Rounds a number down to the nearest power of 2.
     * @param n The number to round
     * @return The nearest power of 2 less than or equal to n
     */
    private static int roundDownToPowerOf2(int n) {
        if (n <= 0) return 4 * 1024 * 1024; // Return minimum size for invalid input
        
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n + 1) >>> 1;
    }
} 
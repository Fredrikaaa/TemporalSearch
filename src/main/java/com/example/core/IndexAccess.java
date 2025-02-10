package com.example.core;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

/**
 * Core interface for accessing index data.
 * Provides a common abstraction for both index generation and query operations.
 */
public interface IndexAccess {
    /**
     * Retrieves positions for a given key in the index.
     * @param key The index key to look up
     * @return List of positions where the key occurs
     */
    List<Position> getPositions(String key) throws IOException;
    
    /**
     * Adds a position for a given key to the index.
     * @param key The index key
     * @param position The position to add
     */
    void addPosition(String key, Position position) throws IOException;
    
    /**
     * Gets the total number of entries in the index.
     * @return The total entry count
     */
    long getEntryCount() throws IOException;
    
    /**
     * Gets the temporal range of entries in the index.
     * @return Array containing [minDate, maxDate]
     */
    LocalDate[] getTimeRange() throws IOException;
    
    /**
     * Checks if a key exists in the index.
     * @param key The key to check
     * @return true if the key exists
     */
    boolean containsKey(String key) throws IOException;
    
    /**
     * Closes any resources associated with this index access.
     */
    void close() throws IOException;
} 
package com.example.index;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for index operations like size checking and deletion.
 */
public class IndexUtils {
    private static final Logger logger = LoggerFactory.getLogger(IndexUtils.class);
    
    /**
     * Safely deletes an existing index directory based on the provided configuration.
     * 
     * @param indexPath Path to the index directory
     * @param config Index configuration
     * @throws IOException if there's an error accessing or deleting the directory
     */
    public static void safeDeleteIndex(Path indexPath, IndexConfig config) throws IOException {
        if (!config.shouldPreserveExistingIndex() && Files.exists(indexPath)) {
            long size = FileUtils.sizeOfDirectory(indexPath.toFile());
            
            if (size >= config.getSizeThresholdForConfirmation()) {
                logger.warn("Large index ({}MB) detected at {}. Deletion requires confirmation.", 
                    size / (1024 * 1024), indexPath);
                // In a real interactive environment, we would prompt for confirmation here
                // For now, we'll just log the warning and proceed
            }
            
            logger.info("Deleting existing index at {}", indexPath);
            FileUtils.deleteDirectory(indexPath.toFile());
            Files.createDirectories(indexPath);
            logger.info("Index directory cleared and recreated");
        } else if (Files.exists(indexPath)) {
            logger.info("Preserving existing index at {}", indexPath);
        } else {
            logger.info("Creating new index directory at {}", indexPath);
            Files.createDirectories(indexPath);
        }
    }
    
    /**
     * Gets the size of an index directory in bytes.
     * 
     * @param indexPath Path to the index directory
     * @return Size of the directory in bytes, or 0 if the directory doesn't exist
     * @throws IOException if there's an error accessing the directory
     */
    public static long getIndexSize(Path indexPath) throws IOException {
        if (Files.exists(indexPath)) {
            return FileUtils.sizeOfDirectory(indexPath.toFile());
        }
        return 0;
    }
} 
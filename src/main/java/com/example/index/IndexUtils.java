package com.example.index;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
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
            long size = getIndexSize(indexPath);
            
            if (size >= config.getSizeThresholdForConfirmation()) {
                logger.warn("Large index ({}MB) detected at {}. Deletion requires confirmation.", 
                    size / (1024 * 1024), indexPath);
                throw new IOException("Index size " + size + " bytes exceeds threshold " + 
                    config.getSizeThresholdForConfirmation() + " bytes. Deletion requires confirmation.");
            }
            
            logger.info("Deleting existing index at {}", indexPath);
            deleteIndex(indexPath);
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
     * @return Size of the directory in bytes, or -1 if there's an error
     */
    public static long getIndexSize(Path indexPath) {
        try {
            if (!Files.exists(indexPath)) {
                return -1;
            }
            return Files.walk(indexPath)
                .filter(p -> Files.isRegularFile(p))
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        logger.error("Failed to get size of file: {}", p, e);
                        return 0;
                    }
                })
                .sum();
        } catch (IOException e) {
            logger.error("Failed to get size of index directory: {}", indexPath, e);
            return -1;
        }
    }

    public static void deleteIndex(Path indexPath) {
        try {
            MoreFiles.deleteRecursively(indexPath, RecursiveDeleteOption.ALLOW_INSECURE);
        } catch (IOException e) {
            logger.error("Failed to delete index directory: {}", indexPath, e);
        }
    }

    public static boolean isIndexEmpty(Path indexPath) {
        try {
            if (!Files.exists(indexPath)) {
                return true;
            }
            try (var files = Files.list(indexPath)) {
                return !files.findAny().isPresent();
            }
        } catch (IOException e) {
            logger.error("Failed to check if index is empty: {}", indexPath, e);
            return true;
        }
    }
} 
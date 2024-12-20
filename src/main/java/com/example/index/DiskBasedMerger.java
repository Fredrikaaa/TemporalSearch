package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Handles external merge sort of temporary indexes with efficient multi-way merging.
 * Manages memory budgets and implements disk-based operations for handling large datasets.
 */
public class DiskBasedMerger implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(DiskBasedMerger.class.getName());
    private static final int DEFAULT_MEMORY_BUDGET_MB = 256;
    private static final int DEFAULT_MERGE_FACTOR = 10;
    
    private final Path tempDir;
    private final int memoryBudgetMB;
    private final int mergeFactor;
    private final ExecutorService executorService;
    
    /**
     * Creates a new DiskBasedMerger with default settings.
     * 
     * @param tempDir Directory for temporary files
     */
    public DiskBasedMerger(Path tempDir) {
        this(tempDir, DEFAULT_MEMORY_BUDGET_MB, DEFAULT_MERGE_FACTOR);
    }
    
    /**
     * Creates a new DiskBasedMerger with custom settings.
     * 
     * @param tempDir Directory for temporary files
     * @param memoryBudgetMB Maximum memory budget in megabytes
     * @param mergeFactor Number of files to merge in each pass
     */
    public DiskBasedMerger(Path tempDir, int memoryBudgetMB, int mergeFactor) {
        this.tempDir = tempDir;
        this.memoryBudgetMB = memoryBudgetMB;
        this.mergeFactor = mergeFactor;
        this.executorService = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors(), mergeFactor)
        );
    }
    
    /**
     * Merges multiple temporary indexes into a single LevelDB database.
     * 
     * @param tempIndexPaths List of paths to temporary index files
     * @param outputPath Path to the output LevelDB database
     * @throws IOException if an I/O error occurs
     */
    public void mergeIndexes(List<Path> tempIndexPaths, Path outputPath) throws IOException {
        if (tempIndexPaths.isEmpty()) {
            return;
        }
        
        // If we have fewer files than merge factor, merge them directly
        if (tempIndexPaths.size() <= mergeFactor) {
            mergeToLevelDB(tempIndexPaths, outputPath);
            return;
        }
        
        // Otherwise, perform multi-way merge in passes
        List<Path> currentPaths = new ArrayList<>(tempIndexPaths);
        while (currentPaths.size() > mergeFactor) {
            List<Path> nextPaths = new ArrayList<>();
            
            // Process files in groups of mergeFactor
            for (int i = 0; i < currentPaths.size(); i += mergeFactor) {
                int end = Math.min(currentPaths.size(), i + mergeFactor);
                List<Path> group = currentPaths.subList(i, end);
                
                // Create temporary output file for this merge group
                Path outputFile = tempDir.resolve("merge_" + UUID.randomUUID() + ".tmp");
                mergeToLevelDB(group, outputFile);
                nextPaths.add(outputFile);
                
                // Clean up merged files
                for (Path path : group) {
                    deleteRecursively(path);
                }
            }
            
            currentPaths = nextPaths;
        }
        
        // Final merge to output database
        mergeToLevelDB(currentPaths, outputPath);
        
        // Clean up remaining temporary files
        for (Path path : currentPaths) {
            deleteRecursively(path);
        }
    }
    
    /**
     * Merges a group of temporary indexes into a single LevelDB database.
     * 
     * @param inputPaths Paths to input temporary indexes
     * @param outputPath Path to output LevelDB database
     * @throws IOException if an I/O error occurs
     */
    private void mergeToLevelDB(List<Path> inputPaths, Path outputPath) throws IOException {
        // Open output database
        Options options = new Options();
        options.createIfMissing(true);
        options.writeBufferSize(memoryBudgetMB * 1024 * 1024 / 2); // Use half of memory budget for write buffer
        
        // First, read all data from input databases
        Map<String, PositionList> mergedData = new HashMap<>();
        
        for (Path path : inputPaths) {
            try (DB db = factory.open(path.toFile(), new Options());
                 DBIterator it = db.iterator()) {
                it.seekToFirst();
                
                while (it.hasNext()) {
                    Map.Entry<byte[], byte[]> entry = it.next();
                    String key = new String(entry.getKey(), StandardCharsets.UTF_8);
                    PositionList positions = PositionList.deserialize(entry.getValue());
                    
                    PositionList existing = mergedData.get(key);
                    if (existing == null) {
                        mergedData.put(key, positions);
                    } else {
                        existing.merge(positions);
                    }
                }
            }
        }
        
        // Now write the merged data to the output database
        try (DB outputDb = factory.open(outputPath.toFile(), options)) {
            for (Map.Entry<String, PositionList> entry : mergedData.entrySet()) {
                outputDb.put(bytes(entry.getKey()), entry.getValue().serialize());
            }
        }
    }
    
    /**
     * Helper class for managing entries in the merge priority queue.
     */
    private static class MergeEntry implements Comparable<MergeEntry> {
        final byte[] key;
        final byte[] value;
        final DBIterator iterator;
        
        MergeEntry(byte[] key, byte[] value, DBIterator iterator) {
            this.key = key;
            this.value = value;
            this.iterator = iterator;
        }
        
        @Override
        public int compareTo(MergeEntry other) {
            return compareKeys(this.key, other.key);
        }
        
        private static int compareKeys(byte[] key1, byte[] key2) {
            String s1 = new String(key1, StandardCharsets.UTF_8);
            String s2 = new String(key2, StandardCharsets.UTF_8);
            return s1.compareTo(s2);
        }
    }
    
    /**
     * Recursively deletes a directory and its contents.
     * 
     * @param path Path to delete
     * @throws IOException if an I/O error occurs
     */
    private void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path entry : stream) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
    
    @Override
    public void close() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
} 
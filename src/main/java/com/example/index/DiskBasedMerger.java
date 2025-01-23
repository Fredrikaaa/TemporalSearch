package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.ArrayListMultimap;
import me.tongfei.progressbar.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import com.example.logging.ProgressTracker;

/**
 * Handles merging of temporary index files using external merge sort.
 * Uses compression to reduce disk space usage and monitors memory to
 * adapt batch sizes during merging.
 */
public class DiskBasedMerger {
    private static final Logger logger = Logger.getLogger(DiskBasedMerger.class.getName());
    private static final int DEFAULT_MERGE_FACTOR = 10;
    private static final long DEFAULT_MEMORY_BUDGET = 100 * 1024 * 1024; // 100MB
    
    private final Path tempDir;
    private final int mergeFactor;
    private final long memoryBudget;
    private final MemoryMonitor memoryMonitor;
    private final ProgressTracker progress;
    
    /**
     * Creates a new DiskBasedMerger with default settings.
     * 
     * @param tempDir Directory for temporary files
     */
    public DiskBasedMerger(Path tempDir) {
        this(tempDir, DEFAULT_MERGE_FACTOR, DEFAULT_MEMORY_BUDGET);
    }
    
    /**
     * Creates a new DiskBasedMerger with custom settings.
     * 
     * @param tempDir Directory for temporary files
     * @param mergeFactor Number of files to merge in each pass
     * @param memoryBudget Maximum memory budget in bytes
     */
    public DiskBasedMerger(Path tempDir, int mergeFactor, long memoryBudget) {
        this.tempDir = tempDir;
        this.mergeFactor = mergeFactor;
        this.memoryBudget = memoryBudget;
        this.memoryMonitor = new MemoryMonitor(0.75); // 75% threshold
        this.progress = new ProgressTracker();
        
        if (!Files.exists(tempDir)) {
            try {
                Files.createDirectories(tempDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create temporary directory", e);
            }
        }
    }
    
    /**
     * Merges a list of temporary index files into a single output file.
     * 
     * @param inputPaths List of paths to temporary index files
     * @param outputPath Path where the merged index should be written
     * @throws IOException If an I/O error occurs
     */
    public void mergeIndexes(List<Path> inputPaths, Path outputPath) throws IOException {
        if (inputPaths.isEmpty()) {
            logger.warning("No input files to merge");
            return;
        }
        
        System.out.printf("Merging %d index files...\n", inputPaths.size());
        
        // Compress input files if they're not already compressed
        List<Path> compressedPaths = new ArrayList<>();
        progress.startBatch(inputPaths.size());
        for (Path path : inputPaths) {
            if (!path.toString().endsWith(".gz")) {
                Path compressedPath = tempDir.resolve(path.getFileName() + ".gz");
                CompressionUtils.compressFile(path, compressedPath);
                compressedPaths.add(compressedPath);
            } else {
                compressedPaths.add(path);
            }
            progress.updateBatch(1);
        }
        progress.completeBatch();
        
        // Perform multi-pass merge if necessary
        List<Path> currentPaths = compressedPaths;
        int passCount = 0;
        while (currentPaths.size() > mergeFactor) {
            passCount++;
            System.out.printf("Starting merge pass %d (%d files)...\n", passCount, currentPaths.size());
            currentPaths = multiPassMerge(currentPaths);
        }
        
        // Final merge to output file
        if (currentPaths.size() > 1) {
            System.out.println("Performing final merge...");
        }
        mergeFiles(currentPaths, outputPath);
        
        // Clean up temporary files
        progress.startBatch(compressedPaths.size());
        for (Path path : compressedPaths) {
            if (!path.equals(outputPath)) {
                Files.deleteIfExists(path);
            }
            progress.updateBatch(1);
        }
        progress.completeBatch();
    }
    
    private List<Path> multiPassMerge(List<Path> paths) throws IOException {
        List<Path> result = new ArrayList<>();
        List<Path> batch = new ArrayList<>();
        int batchCount = 0;
        int totalBatches = (int) Math.ceil((double) paths.size() / mergeFactor);
        
        progress.startBatch(totalBatches);
        for (Path path : paths) {
            batch.add(path);
            if (batch.size() == mergeFactor) {
                Path mergedPath = tempDir.resolve(String.format("merged_%d.gz", batchCount++));
                mergeFiles(batch, mergedPath);
                result.add(mergedPath);
                batch.clear();
                progress.updateBatch(1);
            }
        }
        
        if (!batch.isEmpty()) {
            Path mergedPath = tempDir.resolve(String.format("merged_%d.gz", batchCount));
            mergeFiles(batch, mergedPath);
            result.add(mergedPath);
            progress.updateBatch(1);
        }
        progress.completeBatch();
        
        return result;
    }
    
    private void mergeFiles(List<Path> inputs, Path output) throws IOException {
        // Create readers for all input files
        List<BufferedReader> readers = new ArrayList<>();
        List<String> currentLines = new ArrayList<>();
        
        try {
            // Initialize readers and current lines
            for (Path path : inputs) {
                InputStream in = CompressionUtils.createDecompressionInputStream(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                readers.add(reader);
                String line = reader.readLine();
                currentLines.add(line);
            }
            
            // Create writer for output file
            try (OutputStream out = CompressionUtils.createCompressedOutputStream(output);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
                
                long linesProcessed = 0;
                progress.startBatch(1_000_000);
                
                // Merge while monitoring memory usage
                while (true) {
                    memoryMonitor.update();
                    
                    // Find the smallest current line
                    int smallestIndex = -1;
                    String smallestLine = null;
                    
                    for (int i = 0; i < currentLines.size(); i++) {
                        String line = currentLines.get(i);
                        if (line != null && (smallestLine == null || line.compareTo(smallestLine) < 0)) {
                            smallestLine = line;
                            smallestIndex = i;
                        }
                    }
                    
                    if (smallestIndex == -1) {
                        break; // All files exhausted
                    }
                    
                    // Write the smallest line and read next from that file
                    writer.write(smallestLine);
                    writer.newLine();
                    
                    String nextLine = readers.get(smallestIndex).readLine();
                    currentLines.set(smallestIndex, nextLine);
                    
                    linesProcessed++;
                    if (linesProcessed % 10000 == 0) {
                        progress.updateBatch(10000);
                    }
                }
                
                progress.completeBatch();
            }
        } finally {
            // Close all readers
            for (BufferedReader reader : readers) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.warning("Failed to close reader: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Gets memory usage statistics from the memory monitor.
     * 
     * @return A string containing current memory usage information
     */
    public String getMemoryStats() {
        return memoryMonitor.getMemoryStats();
    }
} 
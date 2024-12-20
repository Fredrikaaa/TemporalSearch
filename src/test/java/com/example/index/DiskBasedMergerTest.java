package com.example.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

class DiskBasedMergerTest {
    @TempDir
    Path tempDir;
    
    private DiskBasedMerger merger;
    private Path mergeDir;
    private List<Path> inputFiles;
    
    @BeforeEach
    void setUp() throws IOException {
        mergeDir = tempDir.resolve("merge");
        Files.createDirectories(mergeDir);
        merger = new DiskBasedMerger(mergeDir);
        inputFiles = new ArrayList<>();
    }
    
    @AfterEach
    void tearDown() throws IOException {
        for (Path file : inputFiles) {
            Files.deleteIfExists(file);
        }
    }
    
    @Test
    void testMergeEmptyFiles() throws IOException {
        // Create empty files
        for (int i = 0; i < 3; i++) {
            Path file = createInputFile(String.format("empty_%d.txt", i), Collections.emptyList());
            inputFiles.add(file);
        }
        
        Path outputPath = mergeDir.resolve("merged.gz");
        merger.mergeIndexes(inputFiles, outputPath);
        
        assertTrue(Files.exists(outputPath), "Output file should be created");
        assertEquals(0, countLines(outputPath), "Merged file should be empty");
    }
    
    @Test
    void testMergeSortedFiles() throws IOException {
        // Create input files with sorted content
        inputFiles.add(createInputFile("file1.txt", Arrays.asList("apple", "dog", "zebra")));
        inputFiles.add(createInputFile("file2.txt", Arrays.asList("banana", "cat", "elephant")));
        inputFiles.add(createInputFile("file3.txt", Arrays.asList("bird", "fish", "lion")));
        
        Path outputPath = mergeDir.resolve("merged.gz");
        merger.mergeIndexes(inputFiles, outputPath);
        
        List<String> expected = Arrays.asList(
            "apple", "banana", "bird", "cat", "dog", "elephant", "fish", "lion", "zebra"
        );
        assertLinesMatch(expected, readLines(outputPath));
    }
    
    @Test
    void testMergeLargeFiles() throws IOException {
        // Create larger input files
        Random random = new Random(42); // Use fixed seed for reproducibility
        int fileCount = 5;
        int linesPerFile = 1000;
        
        for (int i = 0; i < fileCount; i++) {
            List<String> lines = new ArrayList<>();
            for (int j = 0; j < linesPerFile; j++) {
                lines.add(String.format("key_%06d", random.nextInt(1000000)));
            }
            Collections.sort(lines); // Sort lines within each file
            inputFiles.add(createInputFile(String.format("large_%d.txt", i), lines));
        }
        
        Path outputPath = mergeDir.resolve("merged.gz");
        merger.mergeIndexes(inputFiles, outputPath);
        
        // Verify the output is sorted
        List<String> mergedLines = readLines(outputPath);
        assertEquals(fileCount * linesPerFile, mergedLines.size(),
            "Should contain all input lines");
        
        List<String> sortedLines = new ArrayList<>(mergedLines);
        Collections.sort(sortedLines);
        assertEquals(sortedLines, mergedLines,
            "Output should be sorted");
    }
    
    @Test
    void testMemoryMonitoring() throws IOException {
        // Create input files
        inputFiles.add(createInputFile("file1.txt", Arrays.asList("apple", "dog", "zebra")));
        inputFiles.add(createInputFile("file2.txt", Arrays.asList("banana", "cat", "elephant")));
        
        Path outputPath = mergeDir.resolve("merged.gz");
        merger.mergeIndexes(inputFiles, outputPath);
        
        // Verify memory stats are available
        String memoryStats = merger.getMemoryStats();
        assertNotNull(memoryStats, "Memory stats should be available");
        assertTrue(memoryStats.contains("Memory Usage:"),
            "Memory stats should contain usage information");
    }
    
    private Path createInputFile(String name, List<String> lines) throws IOException {
        Path file = mergeDir.resolve(name);
        Files.write(file, lines, StandardCharsets.UTF_8);
        return file;
    }
    
    private List<String> readLines(Path file) throws IOException {
        try (InputStream in = CompressionUtils.createDecompressionInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().collect(java.util.stream.Collectors.toList());
        }
    }
    
    private long countLines(Path file) throws IOException {
        try (InputStream in = CompressionUtils.createDecompressionInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().count();
        }
    }
} 
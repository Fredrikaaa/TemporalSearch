package com.example.index;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IndexUtilsTest {
    @TempDir
    Path tempDir;

    @Test
    void testSafeDeleteIndex() throws IOException {
        Path indexPath = tempDir.resolve("index");
        Files.createDirectories(indexPath);

        // Create some files in the index directory
        Files.writeString(indexPath.resolve("test.txt"), "test content");

        // Test deletion
        IndexConfig config = new IndexConfig.Builder()
            .withPreserveExistingIndex(false)
            .withSizeThresholdForConfirmation(1024 * 1024) // 1MB
            .build();

        IndexUtils.safeDeleteIndex(indexPath, config);

        assertFalse(Files.exists(indexPath));
        
        // Cleanup
        MoreFiles.deleteRecursively(tempDir, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    void testSafeDeleteIndexWithSizeThreshold() throws IOException {
        Path indexPath = tempDir.resolve("index");
        Files.createDirectories(indexPath);

        // Create some files in the index directory
        Files.writeString(indexPath.resolve("test.txt"), "test content");

        // Test deletion with size threshold
        IndexConfig config = new IndexConfig.Builder()
            .withPreserveExistingIndex(false)
            .withSizeThresholdForConfirmation(1) // 1 byte
            .build();

        assertThrows(IOException.class, () -> IndexUtils.safeDeleteIndex(indexPath, config));

        assertTrue(Files.exists(indexPath));
        
        // Cleanup
        MoreFiles.deleteRecursively(indexPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    @Test
    void testGetIndexSize() throws IOException {
        // Create some test files
        Path indexPath = tempDir.resolve("index");
        Files.createDirectories(indexPath);
        Files.writeString(indexPath.resolve("test1.txt"), "test data 1");
        Files.writeString(indexPath.resolve("test2.txt"), "test data 2");
        
        long size = IndexUtils.getIndexSize(indexPath);
        assertTrue(size > 0, "Size should be greater than 0");
        
        // Test non-existent directory
        Path nonExistentPath = tempDir.resolve("non-existent");
        assertEquals(-1, IndexUtils.getIndexSize(nonExistentPath), "Non-existent directory should return -1");
    }
} 
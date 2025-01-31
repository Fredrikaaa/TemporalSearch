package com.example.index;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IndexUtilsTest {
    private Path tempDir;
    private Path indexPath;
    
    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("index-utils-test");
        indexPath = tempDir.resolve("index");
        Files.createDirectories(indexPath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(tempDir.toFile());
    }
    
    @Test
    void testSafeDeleteIndex_WhenPreserveIsFalse() throws IOException {
        // Create some test files
        Files.writeString(indexPath.resolve("test.txt"), "test data");
        assertTrue(Files.exists(indexPath.resolve("test.txt")));
        
        IndexConfig config = new IndexConfig.Builder()
            .withPreserveExistingIndex(false)
            .build();
            
        IndexUtils.safeDeleteIndex(indexPath, config);
        
        assertTrue(Files.exists(indexPath), "Index directory should be recreated");
        assertFalse(Files.exists(indexPath.resolve("test.txt")), "Files should be deleted");
    }
    
    @Test
    void testSafeDeleteIndex_WhenPreserveIsTrue() throws IOException {
        // Create some test files
        Files.writeString(indexPath.resolve("test.txt"), "test data");
        assertTrue(Files.exists(indexPath.resolve("test.txt")));
        
        IndexConfig config = new IndexConfig.Builder()
            .withPreserveExistingIndex(true)
            .build();
            
        IndexUtils.safeDeleteIndex(indexPath, config);
        
        assertTrue(Files.exists(indexPath), "Index directory should exist");
        assertTrue(Files.exists(indexPath.resolve("test.txt")), "Files should be preserved");
    }
    
    @Test
    void testSafeDeleteIndex_WhenDirectoryDoesNotExist() throws IOException {
        FileUtils.deleteDirectory(indexPath.toFile());
        assertFalse(Files.exists(indexPath));
        
        IndexConfig config = new IndexConfig.Builder()
            .withPreserveExistingIndex(false)
            .build();
            
        IndexUtils.safeDeleteIndex(indexPath, config);
        
        assertTrue(Files.exists(indexPath), "Index directory should be created");
    }
    
    @Test
    void testGetIndexSize() throws IOException {
        // Create some test files
        Files.writeString(indexPath.resolve("test1.txt"), "test data 1");
        Files.writeString(indexPath.resolve("test2.txt"), "test data 2");
        
        long size = IndexUtils.getIndexSize(indexPath);
        assertTrue(size > 0, "Size should be greater than 0");
        
        // Test non-existent directory
        Path nonExistentPath = tempDir.resolve("non-existent");
        assertEquals(0, IndexUtils.getIndexSize(nonExistentPath));
    }
} 
package com.example.index;

import com.example.IndexRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class DependencyIndexIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(DependencyIndexIntegrationTest.class);
    
    @TempDir
    Path tempDir;
    
    private Path dbPath;
    private Path indexDir;
    private Path stopwordsPath;
    
    @BeforeEach
    void setUp() throws Exception {
        // Setup test paths
        dbPath = tempDir.resolve("test.db");
        indexDir = tempDir.resolve("index");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        
        // Create test stopwords file
        Files.writeString(stopwordsPath, "the\na\nan\n");
        
        // Create test database with dependencies
        createTestDatabase();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Clean up if needed
    }
    
    private void createTestDatabase() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            
            // Create tables
            stmt.execute("""
                CREATE TABLE documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT
                )
            """);
            
            stmt.execute("""
                CREATE TABLE sentences (
                    id INTEGER PRIMARY KEY,
                    doc_id INTEGER,
                    text TEXT,
                    timestamp INTEGER,
                    FOREIGN KEY(doc_id) REFERENCES documents(document_id)
                )
            """);
            
            stmt.execute("""
                CREATE TABLE dependencies (
                    sentence_id INTEGER,
                    document_id INTEGER,
                    head_token TEXT,
                    head_start INTEGER,
                    head_end INTEGER,
                    dependent_token TEXT,
                    dependent_start INTEGER,
                    dependent_end INTEGER,
                    relation TEXT,
                    FOREIGN KEY(sentence_id) REFERENCES sentences(id),
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
            
            // Insert test data
            stmt.execute("""
                INSERT INTO documents (document_id, timestamp)
                VALUES (1, '2024-01-14T12:00:00Z')
            """);
            
            stmt.execute("""
                INSERT INTO sentences (id, doc_id, text, timestamp)
                VALUES (1, 1, 'The cat sleeps.', 1000)
            """);
            
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token, head_start, head_end,
                    dependent_token, dependent_start, dependent_end, relation)
                VALUES 
                    (1, 1, 'sleeps', 8, 14, 'cat', 4, 7, 'nsubj'),
                    (1, 1, 'cat', 4, 7, 'The', 0, 3, 'det')
            """);
        }
    }
    
    @Test
    void testDependencyIndexGeneration() throws Exception {
        // Run indexing
        IndexRunner.runIndexing(
            dbPath.toString(),
            indexDir.toString(),
            stopwordsPath.toString(),
            100,
            "dependency"
        );
        
        // Verify index was created
        Path dependencyIndexDir = indexDir.resolve("dependency");
        assertTrue(Files.exists(dependencyIndexDir), "Dependency index directory should exist");
        
        // Verify index contents
        try (DependencyIndexGenerator indexer = new DependencyIndexGenerator(
                dependencyIndexDir.toString(), stopwordsPath.toString(), 100,
                DriverManager.getConnection("jdbc:sqlite:" + dbPath))) {
            
            // Check that nsubj relation is indexed but det is filtered
            assertTrue(indexer.hasKey("sleeps\0nsubj\0cat"), 
                "Should contain subject dependency");
            assertFalse(indexer.hasKey("cat\0det\0The"),
                "Should not contain determiner dependency (filtered)");
        }
    }
} 
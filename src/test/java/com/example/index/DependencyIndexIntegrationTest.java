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
                    dependent_token TEXT,
                    relation TEXT,
                    begin_char INTEGER,
                    end_char INTEGER,
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
                INSERT INTO dependencies (document_id, sentence_id, head_token,
                    dependent_token, relation, begin_char, end_char)
                VALUES 
                    (1, 1, 'sleeps', 'cat', 'nsubj', 4, 7),
                    (1, 1, 'cat', 'The', 'det', 0, 3)
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
            assertTrue(indexer.hasKey("sleeps" + BaseIndexGenerator.DELIMITER + "nsubj" + BaseIndexGenerator.DELIMITER + "cat"),
                "Should find subject dependency");
            assertFalse(indexer.hasKey("cat" + BaseIndexGenerator.DELIMITER + "det" + BaseIndexGenerator.DELIMITER + "The"),
                "Should not find blacklisted dependency");
        }
    }
} 
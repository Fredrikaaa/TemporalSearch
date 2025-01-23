package com.example.index;

import com.example.IndexRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DependencyIndexIntegrationTest extends BaseIndexTest {
    protected Path levelDbPath;
    protected Path stopwordsPath;

    @BeforeEach
    @Override
    void setUp() throws Exception {
        super.setUp();
        levelDbPath = tempDir.resolve("test-index/dependency");
        stopwordsPath = tempDir.resolve("stopwords.txt");
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an");
        Files.write(stopwordsPath, stopwords);
        
        setupDatabase();
    }
    
    private void setupDatabase() throws SQLException, Exception {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Insert test data
            stmt.execute("INSERT INTO documents (document_id, timestamp) VALUES (1, '2024-01-20 10:00:00.000')");
            
            // Insert test dependencies
            stmt.execute("""
                INSERT INTO dependencies (document_id, sentence_id, head_token, dependent_token, relation, begin_char, end_char)
                VALUES 
                    (1, 1, 'chases', 'cat', 'nsubj', 4, 7),
                    (1, 1, 'cat', 'The', 'det', 0, 3),
                    (1, 1, 'chases', 'mouse', 'dobj', 11, 16)
            """);
        }
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Clean up if needed
    }
    
    @Test
    void testDependencyIndexGeneration() throws Exception {
        // Run indexing
        IndexRunner.runIndexing(
            tempDir.resolve("test.db").toString(),
            tempDir.resolve("test-index").toString(),
            stopwordsPath.toString(),
            100,
            "dependency"
        );
        
        // Verify index was created
        assertTrue(Files.exists(levelDbPath), "Dependency index directory should exist");
        
        // Verify index contents
        try (DependencyIndexGenerator indexer = new DependencyIndexGenerator(
                levelDbPath.toString(), stopwordsPath.toString(), 100,
                sqliteConn)) {
            
            // Check that nsubj relation is indexed but det is filtered
            String subjectKey = "chases" + BaseIndexGenerator.DELIMITER + "nsubj" + BaseIndexGenerator.DELIMITER + "cat";
            assertTrue(indexer.hasKey(subjectKey), "Should find subject dependency");
            
            String detKey = "cat" + BaseIndexGenerator.DELIMITER + "det" + BaseIndexGenerator.DELIMITER + "The";
            assertFalse(indexer.hasKey(detKey), "Should not find determiner dependency");
        }
    }
} 
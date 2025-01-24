package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import com.example.logging.ProgressTracker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * End-to-end test for the indexing system.
 * Tests the complete pipeline from data insertion to index generation and verification.
 */
public class EndToEndTest extends BaseIndexTest {
    
    @Test
    void testCompleteIndexingPipeline() throws Exception {
        // Create paths for index and stopwords
        Path levelDbPath = tempDir.resolve("test-index");
        Path stopwordsPath = tempDir.resolve("stopwords.txt");
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an", "and", "or", "but");
        Files.write(stopwordsPath, stopwords);
        
        // Insert test data
        insertTestData();
        
        // Run parallel indexing
        long startTime = System.currentTimeMillis();
        
        try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                levelDbPath.toString(),
                stopwordsPath.toString(),
                1000, // batch size
                sqliteConn,
                new ProgressTracker())) {
            generator.generateIndex();
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        verifyIndex(levelDbPath);
    }
    
    private void insertTestData() throws SQLException {
        // Test documents
        String[][] documents = {
            {"The quick brown fox jumps over the lazy dog"},
            {"Pack my box with five dozen liquor jugs"},
            {"How vexingly quick daft zebras jump"},
            {"The five boxing wizards jump quickly"},
            {"Sphinx of black quartz, judge my vow"}
        };
        
        try (PreparedStatement docStmt = sqliteConn.prepareStatement(
                "INSERT INTO documents (document_id, timestamp) VALUES (?, ?)");
             PreparedStatement annStmt = sqliteConn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, token, lemma, pos) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            
            for (int i = 0; i < documents.length; i++) {
                // Insert document
                docStmt.setInt(1, i + 1);
                docStmt.setString(2, LocalDateTime.now().atZone(ZoneId.systemDefault()).toString());
                docStmt.executeUpdate();
                
                // Insert words as annotations
                String[] words = documents[i][0].split("\\s+");
                int charPos = 0;
                for (int j = 0; j < words.length; j++) {
                    String word = words[j].replaceAll("[^a-zA-Z]", "").toLowerCase();
                    if (!word.isEmpty()) {
                        annStmt.setInt(1, i + 1);
                        annStmt.setInt(2, 1);
                        annStmt.setInt(3, charPos);
                        annStmt.setInt(4, charPos + word.length());
                        annStmt.setString(5, word); // Original token
                        annStmt.setString(6, word); // Lemma (simplified)
                        annStmt.setString(7, "NN"); // Simplified POS tag
                        annStmt.executeUpdate();
                    }
                    charPos += words[j].length() + 1;
                }
            }
        }
    }
    
    private void verifyIndex(Path indexPath) throws IOException {
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check some expected words
            Map<String, Integer> expectedCounts = new HashMap<>();
            expectedCounts.put("quick", 2);
            expectedCounts.put("fox", 1);
            expectedCounts.put("jump", 2);
            expectedCounts.put("box", 1);
            expectedCounts.put("wizards", 1);
            expectedCounts.put("sphinx", 1);
            
            for (Map.Entry<String, Integer> entry : expectedCounts.entrySet()) {
                String word = entry.getKey();
                int expectedCount = entry.getValue();
                
                byte[] value = db.get(word.getBytes());
                assertNotNull(value, "Word '" + word + "' should be in index");
                
                PositionList positions = PositionList.deserialize(value);
                assertEquals(expectedCount, positions.getPositions().size(),
                    String.format("Word '%s' should appear %d time(s)", word, expectedCount));
            }
        }
    }
} 
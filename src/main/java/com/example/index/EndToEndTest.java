package com.example.index;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import com.example.logging.ProgressTracker;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.*;
import java.util.*;

public class EndToEndTest {
    public static void main(String[] args) throws Exception {
        // Create temporary directories
        Path tempDir = Files.createTempDirectory("index-test-");
        Path levelDbPath = tempDir.resolve("test-index");
        Path stopwordsPath = tempDir.resolve("stopwords.txt");
        Path sqlitePath = tempDir.resolve("test.db");
        
        // Create stopwords file
        List<String> stopwords = Arrays.asList("the", "a", "an", "and", "or", "but");
        Files.write(stopwordsPath, stopwords);
        
        // Create and set up SQLite database
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath)) {
            setupDatabase(conn);
            insertTestData(conn);
            
            // Run parallel indexing
            System.out.println("Starting parallel indexing...");
            long startTime = System.currentTimeMillis();
            
            try (UnigramIndexGenerator generator = new UnigramIndexGenerator(
                    levelDbPath.toString(),
                    stopwordsPath.toString(),
                    1000, // batch size
                    conn,
                    new ProgressTracker())) {
                generator.generateIndex();
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println("Indexing completed in " + (endTime - startTime) + "ms");
            
            // Verify results
            verifyIndex(levelDbPath);
        }
        
        // Clean up
        Files.walk(tempDir)
             .sorted(Comparator.reverseOrder())
             .forEach(path -> {
                 try {
                     Files.delete(path);
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             });
    }
    
    private static void setupDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE documents (
                    document_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL
                )
            """);
            
            stmt.execute("""
                CREATE TABLE annotations (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    begin_char INTEGER,
                    end_char INTEGER,
                    lemma TEXT,
                    pos TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
        }
    }
    
    private static void insertTestData(Connection conn) throws SQLException {
        // Insert some test documents
        String[][] documents = {
            {"The quick brown fox jumps over the lazy dog"},
            {"Pack my box with five dozen liquor jugs"},
            {"How vexingly quick daft zebras jump"},
            {"The five boxing wizards jump quickly"},
            {"Sphinx of black quartz, judge my vow"}
        };
        
        try (PreparedStatement docStmt = conn.prepareStatement(
                "INSERT INTO documents (document_id, timestamp) VALUES (?, ?)");
             PreparedStatement annStmt = conn.prepareStatement(
                "INSERT INTO annotations (document_id, sentence_id, begin_char, end_char, lemma, pos) VALUES (?, ?, ?, ?, ?, ?)")) {
            
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
                        annStmt.setString(5, word);
                        annStmt.setString(6, "NN"); // Simplified POS tag
                        annStmt.executeUpdate();
                    }
                    charPos += words[j].length() + 1;
                }
            }
        }
    }
    
    private static void verifyIndex(Path indexPath) throws IOException {
        System.out.println("\nVerifying index contents:");
        Options options = new Options();
        try (DB db = factory.open(indexPath.toFile(), options)) {
            // Check some expected words
            String[] wordsToCheck = {"quick", "fox", "jump", "box", "wizard", "sphinx"};
            for (String word : wordsToCheck) {
                byte[] value = db.get(word.getBytes());
                if (value != null) {
                    PositionList positions = PositionList.deserialize(value);
                    System.out.printf("'%s' appears in %d positions%n", 
                        word, positions.getPositions().size());
                } else {
                    System.out.printf("WARNING: '%s' not found in index%n", word);
                }
            }
        }
    }
} 
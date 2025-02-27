package com.example.query.snippet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for diagnosing database issues related to snippets.
 */
public class DatabaseDiagnostic {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDiagnostic.class);
    
    /**
     * Checks the database for issues that might prevent snippets from working.
     * 
     * @param dbPath Path to the database file
     */
    public static void checkDatabase(String dbPath) {
        logger.info("Checking database at: {}", dbPath);
        
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            logger.info("Successfully connected to database");
            
            // Check that required tables exist
            checkRequiredTables(connection);
            
            // Check sample records
            checkSampleRecords(connection);
            
        } catch (SQLException e) {
            logger.error("Failed to connect to database: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Checks that required tables exist in the database.
     * 
     * @param connection Database connection
     * @throws SQLException if a database error occurs
     */
    private static void checkRequiredTables(Connection connection) throws SQLException {
        List<String> requiredTables = List.of("documents", "sentences", "annotations");
        List<String> missingTables = new ArrayList<>();
        
        try (ResultSet rs = connection.getMetaData().getTables(null, null, "%", null)) {
            List<String> existingTables = new ArrayList<>();
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                existingTables.add(tableName);
            }
            
            for (String requiredTable : requiredTables) {
                if (!existingTables.contains(requiredTable)) {
                    missingTables.add(requiredTable);
                }
            }
        }
        
        if (missingTables.isEmpty()) {
            logger.info("All required tables exist");
        } else {
            logger.error("Missing required tables: {}", missingTables);
        }
        
        // Check table structure
        for (String table : requiredTables) {
            if (!missingTables.contains(table)) {
                try (Statement stmt = connection.createStatement()) {
                    ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + table + ")");
                    List<String> columns = new ArrayList<>();
                    while (rs.next()) {
                        columns.add(rs.getString("name"));
                    }
                    logger.info("Table {} has columns: {}", table, columns);
                }
            }
        }
    }
    
    /**
     * Checks sample records in the database to see if they contain the expected data.
     * 
     * @param connection Database connection
     * @throws SQLException if a database error occurs
     */
    private static void checkSampleRecords(Connection connection) throws SQLException {
        // Check for documents
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM documents");
            if (rs.next()) {
                int count = rs.getInt("count");
                logger.info("Found {} documents in the database", count);
            }
        }
        
        // Check for sentences
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM sentences");
            if (rs.next()) {
                int count = rs.getInt("count");
                logger.info("Found {} sentences in the database", count);
            }
        }
        
        // Check for annotations
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as count FROM annotations");
            if (rs.next()) {
                int count = rs.getInt("count");
                logger.info("Found {} annotations in the database", count);
            }
        }
        
        // Check for a sample document's sentences
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT document_id FROM documents LIMIT 1");
            if (rs.next()) {
                int documentId = rs.getInt("document_id");
                logger.info("Checking sentences for document ID: {}", documentId);
                
                try (PreparedStatement pstmt = connection.prepareStatement(
                    "SELECT sentence_id, text FROM sentences WHERE document_id = ? LIMIT 5")) {
                    pstmt.setInt(1, documentId);
                    ResultSet sentRs = pstmt.executeQuery();
                    int count = 0;
                    while (sentRs.next()) {
                        count++;
                        int sentenceId = sentRs.getInt("sentence_id");
                        String text = sentRs.getString("text");
                        logger.info("Document {}, Sentence {}: {}", documentId, sentenceId, 
                            text.length() > 50 ? text.substring(0, 50) + "..." : text);
                    }
                    
                    if (count == 0) {
                        logger.warn("No sentences found for document ID: {}", documentId);
                    }
                }
            } else {
                logger.warn("No documents found in the database");
            }
        }
        
        // Check for a sample annotation
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT document_id, sentence_id, token, begin_char, end_char FROM annotations LIMIT 5");
            int count = 0;
            while (rs.next()) {
                count++;
                int documentId = rs.getInt("document_id");
                int sentenceId = rs.getInt("sentence_id");
                String token = rs.getString("token");
                int beginChar = rs.getInt("begin_char");
                int endChar = rs.getInt("end_char");
                
                logger.info("Sample annotation: doc={}, sent={}, token='{}', position=[{},{}]", 
                    documentId, sentenceId, token, beginChar, endChar);
            }
            
            if (count == 0) {
                logger.warn("No annotations found in the database");
            }
        }
    }
} 
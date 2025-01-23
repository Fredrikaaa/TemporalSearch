package com.example.index;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Base test class providing common utilities for index testing.
 * Handles database setup, cleanup, and common test data creation.
 */
public abstract class BaseIndexTest {
    protected Path tempDir;
    protected Connection sqliteConn;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("index-test-");
        sqliteConn = createTestDatabase();
        createBasicTables();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (sqliteConn != null) {
            sqliteConn.close();
        }
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    protected Connection createTestDatabase() throws Exception {
        return DriverManager.getConnection("jdbc:sqlite:" + tempDir.resolve("test.db"));
    }

    protected void createBasicTables() throws Exception {
        try (Statement stmt = sqliteConn.createStatement()) {
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
                    token TEXT,
                    lemma TEXT,
                    pos TEXT,
                    ner TEXT,
                    normalized_ner TEXT,
                    annotation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);

            stmt.execute("""
                CREATE TABLE dependencies (
                    document_id INTEGER,
                    sentence_id INTEGER,
                    begin_char INTEGER,
                    end_char INTEGER,
                    head_token TEXT,
                    dependent_token TEXT,
                    relation TEXT,
                    FOREIGN KEY(document_id) REFERENCES documents(document_id)
                )
            """);
        }
    }
} 
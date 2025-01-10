package com.example.index;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.*;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class DependencyIndexGeneratorTest {
    private Connection conn;
    private File levelDbFile;

    @Before
    public void setUp() throws Exception {
        // Create an in-memory SQLite database
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        setupDatabase(conn);
        insertTestDependencies(conn);

        // Create a temporary LevelDB directory
        levelDbFile = Files.createTempDirectory("dep-index-test").toFile();
    }

    @After
    public void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
        // Remove the LevelDB directory on cleanup
        deleteRecursively(levelDbFile);
    }

    @Test
    public void testDependencyIndexGeneration() throws Exception {
        try (DependencyIndexGenerator generator = new DependencyIndexGenerator(
                levelDbFile.getAbsolutePath(),
                // Provide an empty stopwords file or path for simplicity
                createEmptyStopwordsFile().getAbsolutePath(),
                100, // batch size
                conn)) {
            generator.generateIndex();
        }

        // Verify entries in LevelDB
        Options options = new Options();
        options.createIfMissing(false);
        try (DB db = factory.open(levelDbFile, options)) {
            String key1 = "jump\0nsubj\0fox";
            byte[] val1 = db.get(key1.getBytes());
            assertNotNull("Expected key1 to be in index", val1);

            String key2 = "root\0obj\0dog";
            byte[] val2 = db.get(key2.getBytes());
            assertNotNull("Expected key2 to be in index", val2);

            // Confirm that a blacklisted relation key does not appear
            String blacklistedKey = "root\0punct\0!";
            assertNull("Should not find blacklisted relation in index",
                    db.get(blacklistedKey.getBytes()));
        }
    }

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    private void setupDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE documents (document_id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL)");
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

    private void insertTestDependencies(Connection conn) throws SQLException {
        try (PreparedStatement docStmt = conn.prepareStatement(
                "INSERT INTO documents (document_id, timestamp) VALUES (?,?)");
                PreparedStatement depStmt = conn.prepareStatement("""
                            INSERT INTO dependencies
                            (document_id, sentence_id, begin_char, end_char, head_token, dependent_token, relation)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """)) {

            // Insert a single doc with a known timestamp
            docStmt.setInt(1, 1);
            docStmt.setString(2, ZonedDateTime.now().toString());
            docStmt.executeUpdate();

            // Insert sample dependencies (some with blacklisted relations)
            insertDep(depStmt, 1, 1, 0, 4, "jump", "fox", "nsubj");
            insertDep(depStmt, 1, 1, 5, 8, "root", "dog", "obj");
            insertDep(depStmt, 1, 1, 9, 9, "root", "!", "punct"); // blacklisted
        }
    }

    private void insertDep(
            PreparedStatement stmt, int docId, int sentId,
            int begin, int end, String head, String depTok, String rel) throws SQLException {
        stmt.setInt(1, docId);
        stmt.setInt(2, sentId);
        stmt.setInt(3, begin);
        stmt.setInt(4, end);
        stmt.setString(5, head);
        stmt.setString(6, depTok);
        stmt.setString(7, rel);
        stmt.executeUpdate();
    }

    private File createEmptyStopwordsFile() throws Exception {
        File tempFile = Files.createTempFile("stopwords", ".txt").toFile();
        tempFile.deleteOnExit();
        return tempFile;
    }

    private void deleteRecursively(File file) throws Exception {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                deleteRecursively(f);
            }
        }
        file.delete();
    }
}

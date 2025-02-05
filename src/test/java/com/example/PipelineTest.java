package com.example;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

@DisplayName("Pipeline Integration Tests")
public class PipelineTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int TOTAL_DOCS = 20;
    
    protected Path tempDir;
    protected Path jsonFile;
    protected Path dbFile;
    protected Path indexDir;
    protected Path stopwordsFile;
    protected Connection sqliteConn;

    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directory
        tempDir = Files.createTempDirectory("pipeline-test-");
        
        // Setup paths
        jsonFile = createTestData(tempDir);
        dbFile = tempDir.resolve("test.db");
        indexDir = tempDir.resolve("indexes");
        stopwordsFile = createStopwordsFile(tempDir);
        
        // Create database connection
        sqliteConn = createTestDatabase();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (sqliteConn != null) {
            sqliteConn.close();
        }
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    protected Connection createTestDatabase() throws Exception {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
    }

    private Path createStopwordsFile(Path tempDir) throws IOException {
        Path stopwordsFile = tempDir.resolve("stopwords.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(stopwordsFile.toFile()))) {
            // Add some common stopwords
            String[] stopwords = {
                "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
                "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
                "to", "was", "were", "will", "with"
            };
            for (String word : stopwords) {
                writer.write(word);
                writer.newLine();
            }
        }
        return stopwordsFile;
    }

    @Nested
    @DisplayName("Full Pipeline Tests")
    class FullPipelineTests {
        @Test
        @DisplayName("Full pipeline processes all stages successfully")
        void testFullPipeline() throws Exception {
            Pipeline.runPipeline(createPipelineArgs("all", true));
            verifyConversionStage(TOTAL_DOCS);
            verifyAnnotationStage(TOTAL_DOCS);
            verifyIndexingStage();
        }

        @Test
        @DisplayName("Pipeline with limit processes correct number of documents")
        void testPipelineWithLimit() throws Exception {
            int limit = 5;
            String[] args = {
                "-s", "all",
                "-f", jsonFile.toString(),
                "-d", dbFile.toString(),
                "-i", indexDir.toString(),
                "--stopwords", stopwordsFile.toString(),
                "-l", String.valueOf(limit),
                "--recreate"
            };
            Pipeline.runPipeline(args);

            verifyConversionStage(limit);
            verifyAnnotationStage(limit);
            verifyIndexingStage();
        }
    }

    @Nested
    @DisplayName("Individual Stage Tests")
    class StageTests {
        @Test
        @DisplayName("Conversion stage creates database correctly")
        void testConversionStage() throws Exception {
            String[] args = {
                "-s", "convert",
                "-f", jsonFile.toString(),
                "-d", dbFile.toString(),
                "--recreate"
            };
            Pipeline.runPipeline(args);

            verifyConversionStage(TOTAL_DOCS);
            verifyNoAnnotations();
            verifyNoIndexes();
        }

        @Test
        @DisplayName("Annotation stage processes documents correctly")
        void testAnnotationStage() throws Exception {
            // First run conversion
            Pipeline.runPipeline(new String[]{
                "-s", "convert",
                "-f", jsonFile.toString(),
                "-d", dbFile.toString(),
                "--recreate"
            });

            // Then run annotation
            String[] args = {
                "-s", "annotate",
                "-d", dbFile.toString(),
                "-b", "5",
                "-t", "2"
            };
            Pipeline.runPipeline(args);

            verifyConversionStage(TOTAL_DOCS);
            verifyAnnotationStage(TOTAL_DOCS);
            verifyNoIndexes();
        }

        @Test
        @DisplayName("Indexing stage creates all index types")
        void testIndexingStage() throws Exception {
            // Run conversion and annotation first
            setupConversionAndAnnotation();

            // Test indexing with all types
            String[] args = {
                "-s", "index",
                "-d", dbFile.toString(),
                "-i", indexDir.toString(),
                "--stopwords", stopwordsFile.toString(),
                "-y", "all"
            };
            Pipeline.runPipeline(args);

            verifyIndexingStage();
        }

        @Test
        @DisplayName("Indexing stage creates specific index type")
        void testSpecificIndexType() throws Exception {
            // Run conversion and annotation first
            setupConversionAndAnnotation();

            // Test specific index type
            String[] args = {
                "-s", "index",
                "-d", dbFile.toString(),
                "-i", indexDir.toString(),
                "--stopwords", stopwordsFile.toString(),
                "-y", "unigram"
            };
            Pipeline.runPipeline(args);

            // Verify only unigram index exists
            assertTrue(indexDir.resolve("unigram").toFile().exists(),
                "Unigram index should exist");
            assertFalse(indexDir.resolve("bigram").toFile().exists(),
                "Bigram index should not exist");
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {
        @Test
        @DisplayName("Pipeline handles missing input file")
        void testMissingInputFile() {
            String[] args = {
                "-s", "convert",
                "-f", "nonexistent.json",
                "-d", dbFile.toString()
            };
            Exception exception = assertThrows(FileNotFoundException.class, 
                () -> Pipeline.runPipeline(args));
            assertTrue(exception.getMessage().contains("nonexistent.json"));
        }

        @Test
        @DisplayName("Pipeline handles invalid stage")
        void testInvalidStage() {
            String[] args = {
                "-s", "invalid",
                "-f", jsonFile.toString(),
                "-d", dbFile.toString()
            };
            ArgumentParserException exception = assertThrows(ArgumentParserException.class, 
                () -> Pipeline.runPipeline(args));
            assertTrue(exception.getMessage().toLowerCase().contains("invalid"));
        }

        @Test
        @DisplayName("Pipeline handles missing required arguments")
        void testMissingRequiredArgs() {
            String[] args = {"-s", "convert"};
            ArgumentParserException exception = assertThrows(ArgumentParserException.class, 
                () -> Pipeline.runPipeline(args));
            assertTrue(exception.getMessage().contains("required"));
        }
    }

    // Helper methods
    private Path createTestData(Path tempDir) throws IOException {
        Path jsonFile = tempDir.resolve("test.json");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(jsonFile.toFile()))) {
            // Create test documents with more realistic content
            for (int i = 0; i < TOTAL_DOCS; i++) {
                ObjectNode doc = MAPPER.createObjectNode()
                    .put("title", "Title " + i)
                    .put("text", String.format(
                        "This is test document %d. It contains sample text for testing NLP features. " +
                        "The quick brown fox jumps over the lazy dog. " +
                        "This document was created on January 1st, 2024. " +
                        "OpenAI's GPT models have revolutionized natural language processing.",
                        i))
                    .put("timestamp", "2024-01-01");
                writer.write(doc.toString());
                writer.newLine();
            }
        }
        return jsonFile;
    }

    private void setupConversionAndAnnotation() throws Exception {
        Pipeline.runPipeline(new String[]{
            "-s", "convert",
            "-f", jsonFile.toString(),
            "-d", dbFile.toString(),
            "--recreate"
        });
        Pipeline.runPipeline(new String[]{
            "-s", "annotate",
            "-d", dbFile.toString()
        });
    }

    private void verifyConversionStage(int expectedCount) throws SQLException {
        // Check total documents
        try (Statement stmt = sqliteConn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM documents");
            assertTrue(rs.next());
            assertEquals(expectedCount, rs.getInt(1), 
                "Database should contain exactly " + expectedCount + " documents");

            // Verify document content
            rs = stmt.executeQuery("SELECT title, text FROM documents ORDER BY document_id LIMIT 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString("title"), "Document title should not be null");
            assertNotNull(rs.getString("text"), "Document text should not be null");
        }
    }

    private void verifyAnnotationStage(int expectedCount) throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            // Check annotations exist for each document
            ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(DISTINCT document_id) FROM annotations");
            assertTrue(rs.next());
            assertEquals(expectedCount, rs.getInt(1), 
                "Should have annotations for exactly " + expectedCount + " documents");

            // Verify annotation content
            rs = stmt.executeQuery(
                "SELECT DISTINCT lemma, pos FROM annotations LIMIT 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString("lemma"), "Annotation lemma should not be null");
            assertNotNull(rs.getString("pos"), "Annotation POS should not be null");
        }
    }

    private void verifyIndexingStage() {
        // Verify index directories were created
        String[] indexTypes = {"unigram", "bigram", "trigram", "dependency", "ner_date", "pos"};
        for (String type : indexTypes) {
            Path indexPath = indexDir.resolve(type);
            assertTrue(indexPath.toFile().exists(), 
                type + " index directory should exist");
            assertTrue(indexPath.toFile().list().length > 0,
                type + " index should not be empty");
        }
    }

    private void verifyNoAnnotations() throws SQLException {
        try (Statement stmt = sqliteConn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='annotations'");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "Annotations table should not exist");
        }
    }

    private void verifyNoIndexes() {
        if (indexDir.toFile().exists()) {
            assertEquals(0, indexDir.toFile().list().length,
                "Index directory should be empty");
        }
    }

    private String[] createPipelineArgs(String stage, boolean includeIndexing) {
        return new String[]{
            "-s", stage,
            "-f", jsonFile.toString(),
            "-d", dbFile.toString(),
            "-i", indexDir.toString(),
            "--stopwords", stopwordsFile.toString(),
            "--recreate"
        };
    }
} 
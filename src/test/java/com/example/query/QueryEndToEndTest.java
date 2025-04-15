package com.example.query;

import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.core.IndexAccessInterface;
import com.example.core.index.MockIndexAccess;
import com.example.query.executor.ConditionExecutorFactory;
import com.example.query.executor.QueryExecutionException;
import com.example.query.executor.QueryExecutor;
import com.example.query.executor.QueryResult;
import com.example.query.model.Query;
import com.example.query.QueryParseException;
import com.example.query.QueryParser;
import com.example.query.result.ResultGenerationException;
import com.example.query.result.TableResultService;
import com.example.query.sqlite.SqliteAccessor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import tech.tablesaw.api.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for query parsing, execution, and result generation.
 * Uses mock indexes for predictable results.
 */
public class QueryEndToEndTest {

    @TempDir
    static Path tempDir;

    private static QueryExecutor queryExecutor;
    private static TableResultService tableResultService;
    private static MockIndexAccess mockUnigramIndex;
    private static MockIndexAccess mockBigramIndex;
    private static MockIndexAccess mockTrigramIndex;
    private static Map<String, IndexAccessInterface> mockIndexes;
    private static QueryParser queryParser;

    private static final char DELIMITER = '\0';

    @BeforeAll
    public static void setUp() throws IOException, IndexAccessException {
        // Use a temporary directory for mock indexes
        File indexBasePath = tempDir.resolve("testIndexes").toFile();
        indexBasePath.mkdirs();
        
        // Initialize SqliteAccessor before creating indexes that might need it
        SqliteAccessor.initialize(indexBasePath.getAbsolutePath());
        
        // Create a mock index instance
        mockUnigramIndex = new MockIndexAccess();
        mockUnigramIndex.addTestData("apple", 1, 1, 0, 5);
        mockUnigramIndex.addTestData("apple", 2, 1, 10, 15);
        mockUnigramIndex.addTestData("banana", 2, 2, 20, 25);
        mockUnigramIndex.addTestData("test", 0, 0, 0, 4); // For SentenceGranularityTest
        mockUnigramIndex.addTestData("test", 1, 1, 0, 4); // For SentenceGranularityTest
        mockUnigramIndex.addTestData("window", 0, 1, 0, 6); // For SentenceGranularityTest
        mockUnigramIndex.addTestData("window", 0, 3, 0, 6); // For SentenceGranularityTest
        mockUnigramIndex.addTestData("grape", 3, 1, 5, 10); // For single quote test

        // Create and populate mock bigram index
        mockBigramIndex = new MockIndexAccess();
        // Using lowercase, lemmatized forms with null byte delimiter
        mockBigramIndex.addTestData("read" + DELIMITER + "monkey", 3, 1, 10, 20); // For space/comma test
        mockBigramIndex.addTestData("big" + DELIMITER + "cat", 4, 1, 0, 6); // For bigram test
        
        // Create and populate mock trigram index
        mockTrigramIndex = new MockIndexAccess();
        mockTrigramIndex.addTestData("the" + DELIMITER + "quick" + DELIMITER + "fox", 5, 1, 0, 15); // For trigram test

        // Update the map of indexes
        mockIndexes = Map.of(
            "unigram", mockUnigramIndex,
            "bigram", mockBigramIndex,
            "trigram", mockTrigramIndex
        );
        
        // Initialize executor and result service
        ConditionExecutorFactory factory = new ConditionExecutorFactory();
        queryExecutor = new QueryExecutor(factory);
        tableResultService = new TableResultService();
        queryParser = new QueryParser();
        
        System.out.println("End-to-End Test Setup Complete.");
    }

    @AfterAll
    public static void tearDown() throws IOException {
        System.out.println("End-to-End Test Teardown Complete.");
    }

    @Test
    public void testSimpleContainsQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS(\"apple\")";
        Query query = queryParser.parse(queryString);

        // Execute query
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        // Assertions on QueryResult
        assertNotNull(result);
        assertFalse(result.getAllDetails().isEmpty(), "Expected results for 'apple'");
        assertEquals(2, result.getAllDetails().size()); // Doc 1 and Doc 2
        assertEquals(Query.Granularity.DOCUMENT, result.getGranularity());
        assertTrue(result.getAllDetails().stream().anyMatch(d -> d.getDocumentId() == 1));
        assertTrue(result.getAllDetails().stream().anyMatch(d -> d.getDocumentId() == 2));

        // Assertions on generated Table
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertNotNull(resultTable);
        assertEquals(2, resultTable.rowCount());
        // Check default columns (document_id)
        assertTrue(resultTable.columnNames().contains("document_id")); // Use literal string
        // Access as IntColumn and compare integer values
        assertEquals(1, resultTable.intColumn("document_id").get(0)); 
        assertEquals(2, resultTable.intColumn("document_id").get(1));
    }
    
     @Test
    public void testContainsNoMatchQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS(\"nonexistent\")";
        Query query = queryParser.parse(queryString);
        
        QueryResult result = queryExecutor.execute(query, mockIndexes);
        
        assertNotNull(result);
        assertTrue(result.getAllDetails().isEmpty(), "Expected no results for 'nonexistent'");
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertNotNull(resultTable);
        assertEquals(0, resultTable.rowCount());
    }
    
    @Test
    public void testContainsSingleQuote() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS('grape')";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertFalse(result.getAllDetails().isEmpty(), "Expected results for 'grape'");
        assertEquals(1, result.getAllDetails().size());
        assertEquals(3, result.getAllDetails().get(0).getDocumentId());
    }

    @Test
    public void testContainsBigramWithSpace() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        // Assumes index contains lemmatized "read\0monkey"
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS(\"read monkey\")";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertFalse(result.getAllDetails().isEmpty(), "Expected results for 'read monkey'");
        assertEquals(1, result.getAllDetails().size());
        assertEquals(3, result.getAllDetails().get(0).getDocumentId());
    }
    
    @Test
    public void testContainsBigramWithComma() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        // Assumes index contains lemmatized "read\0monkey"
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS(\"read\", \"monkey\")";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertFalse(result.getAllDetails().isEmpty(), "Expected results for 'read, monkey'");
        assertEquals(1, result.getAllDetails().size());
        assertEquals(3, result.getAllDetails().get(0).getDocumentId());
    }
    
    @Test
    public void testContainsTrigramWithSpace() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        // Assumes index contains lemmatized "the\0quick\0fox"
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS(\"the quick fox\")";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertFalse(result.getAllDetails().isEmpty(), "Expected results for 'the quick fox'");
        assertEquals(1, result.getAllDetails().size());
        assertEquals(5, result.getAllDetails().get(0).getDocumentId());
    }
    
    @Test
    public void testContainsTrigramWithComma() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        // Assumes index contains lemmatized "the\0quick\0fox"
        String queryString = "SELECT TITLE FROM source WHERE CONTAINS(\"the\", \"quick\", \"fox\")";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertFalse(result.getAllDetails().isEmpty(), "Expected results for 'the, quick, fox'");
        assertEquals(1, result.getAllDetails().size());
        assertEquals(5, result.getAllDetails().get(0).getDocumentId());
    }

    // Add more end-to-end tests for different conditions, granularity, joins etc.
} 
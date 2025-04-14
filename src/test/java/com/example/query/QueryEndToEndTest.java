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
    private static Map<String, IndexAccessInterface> mockIndexes;
    private static QueryParser queryParser;

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

        // Create the map of indexes directly
        mockIndexes = Map.of("unigram", mockUnigramIndex);
        
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
    
    // Add more end-to-end tests for different conditions, granularity, joins etc.
} 
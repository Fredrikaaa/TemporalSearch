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
    private static MockIndexAccess mockNerIndex;
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

        // Create and populate mock NER index (values are lowercase)
        mockNerIndex = new MockIndexAccess();
        mockNerIndex.addTestData("PERSON" + DELIMITER + "albert einstein", 6, 1, 0, 15);
        mockNerIndex.addTestData("PERSON" + DELIMITER + "marie curie", 6, 2, 20, 30);
        mockNerIndex.addTestData("PERSON" + DELIMITER + "isaac newton", 7, 1, 5, 17);
        mockNerIndex.addTestData("ORGANIZATION" + DELIMITER + "google", 7, 2, 40, 46);
        mockNerIndex.addTestData("LOCATION" + DELIMITER + "london", 8, 1, 0, 6);
        mockNerIndex.addTestData("NUMBER" + DELIMITER + "42", 8, 2, 10, 12);
        mockNerIndex.addTestData("ORDINAL" + DELIMITER + "first", 9, 1, 0, 5);
        mockNerIndex.addTestData("DURATION" + DELIMITER + "3 years", 9, 2, 10, 17);
        mockNerIndex.addTestData("SET" + DELIMITER + "weekly", 10, 1, 0, 6);

        // Update the map of indexes
        mockIndexes = Map.of(
            "unigram", mockUnigramIndex,
            "bigram", mockBigramIndex,
            "trigram", mockTrigramIndex,
            "ner", mockNerIndex
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

    // --- NER Tests --- 

    @Test
    public void testNerSimpleTypeQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT TITLE FROM source WHERE NER(PERSON)";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertEquals(3, result.getAllDetails().size(), "Expected 3 PERSON entities");
        Set<Integer> docIds = result.getDetailsByDocId().keySet();
        assertEquals(Set.of(6, 7), docIds, "Expected results in docs 6 and 7");
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(2, resultTable.rowCount()); // Grouped by document
    }
    
    @Test
    public void testNerTypeWithTargetQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT TITLE FROM source WHERE NER(PERSON, 'Albert Einstein')"; // Case mismatch intentional
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertEquals(1, result.getAllDetails().size(), "Expected 1 specific PERSON entity");
        assertEquals(6, result.getAllDetails().get(0).getDocumentId());
        assertEquals("albert einstein", result.getAllDetails().get(0).value());
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(1, resultTable.rowCount()); 
        assertEquals(6, resultTable.intColumn("document_id").get(0));
    }
    
    @Test
    public void testNerTypeWithTargetNoMatchQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT TITLE FROM source WHERE NER(PERSON, 'Non Existent')";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertTrue(result.getAllDetails().isEmpty(), "Expected no results");
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(0, resultTable.rowCount()); 
    }
    
    @Test
    public void testNerVariableBindingQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT ?person FROM source WHERE NER(PERSON) AS ?person";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertEquals(3, result.getAllDetails().size(), "Expected 3 PERSON entities for binding");
        Set<Integer> docIds = result.getDetailsByDocId().keySet();
        assertEquals(Set.of(6, 7), docIds);
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(2, resultTable.rowCount()); // Grouped by doc
        assertTrue(resultTable.columnNames().contains("?person"));
        // Values in the table will be one of the entities from the doc (grouping picks one)
        Set<String> expectedValues = Set.of("albert einstein", "marie curie", "isaac newton");
        assertTrue(expectedValues.contains(resultTable.stringColumn("?person").get(0).toLowerCase()));
        assertTrue(expectedValues.contains(resultTable.stringColumn("?person").get(1).toLowerCase()));
    }
    
    @Test
    public void testNerVariableBindingWithTargetQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT ?org FROM source WHERE NER(ORGANIZATION, 'google') AS ?org";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertEquals(1, result.getAllDetails().size(), "Expected 1 specific ORG entity");
        assertEquals(7, result.getAllDetails().get(0).getDocumentId());
        assertEquals("google", result.getAllDetails().get(0).value());
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(1, resultTable.rowCount()); 
        assertEquals(7, resultTable.intColumn("document_id").get(0));
        assertEquals("google", resultTable.stringColumn("?org").get(0));
    }
    
    @Test
    public void testNerNewTypeOrdinalQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT ?ord FROM source WHERE NER(ORDINAL, 'first') AS ?ord";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertEquals(1, result.getAllDetails().size(), "Expected 1 specific ORDINAL entity");
        assertEquals(9, result.getAllDetails().get(0).getDocumentId());
        assertEquals("first", result.getAllDetails().get(0).value());
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(1, resultTable.rowCount());
        assertEquals("first", resultTable.stringColumn("?ord").get(0));
    }
    
    @Test
    public void testNerNewTypeNumberQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        String queryString = "SELECT ?num FROM source WHERE NER(NUMBER) AS ?num";
        Query query = queryParser.parse(queryString);
        QueryResult result = queryExecutor.execute(query, mockIndexes);

        assertNotNull(result);
        assertEquals(1, result.getAllDetails().size(), "Expected 1 NUMBER entity");
        assertEquals(8, result.getAllDetails().get(0).getDocumentId());
        assertEquals("42", result.getAllDetails().get(0).value());
        
        Table resultTable = tableResultService.generateTable(query, result, mockIndexes);
        assertEquals(1, resultTable.rowCount());
        assertEquals("42", resultTable.stringColumn("?num").get(0));
    }
    
    @Test
    public void testNerWildcardQuery() throws QueryParseException, QueryExecutionException, ResultGenerationException {
        // Wildcard is not fully implemented for search/binding yet, only validation
        // This test assumes it might become valid later, or checks current behavior.
        // Modify based on expected behavior of wildcard in executor.
        String queryString = "SELECT TITLE FROM source WHERE NER(*)"; 
        
        // For now, expect validation error if wildcard isn't handled by executor
        // If executor handles it by searching all NER index entries:
        // Query query = queryParser.parse(queryString);
        // QueryResult result = queryExecutor.execute(query, mockIndexes);
        // assertNotNull(result);
        // assertEquals(9, result.getAllDetails().size()); // Total entities added
        // assertEquals(Set.of(6, 7, 8, 9, 10), result.getDocumentIds());
        
        // Current expectation: Parsing might work, execution might fail depending on wildcard impl.
        assertThrows(QueryExecutionException.class, () -> {
             Query query = queryParser.parse(queryString);
             queryExecutor.execute(query, mockIndexes);
        }, "Wildcard NER(*) execution is not fully supported yet");
    }
} 
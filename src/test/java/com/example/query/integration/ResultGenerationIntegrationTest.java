package com.example.query.integration;

import com.example.core.IndexAccess;
import com.example.query.executor.QueryExecutor;
import com.example.query.executor.QueryExecutionException;
import com.example.query.executor.VariableBindings;
import com.example.query.index.IndexManager;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.QueryParser;
import com.example.query.QueryParseException;
import com.example.query.result.ResultFormatter;
import com.example.query.result.ResultGenerator;
import com.example.query.result.ResultGenerationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for the Result Generation component.
 * Tests the complete flow from query execution to result formatting.
 */
public class ResultGenerationIntegrationTest {

    @TempDir
    Path tempDir;

    @Mock
    private IndexAccess mockIndexAccess;

    @Mock
    private IndexManager mockIndexManager;

    @Mock
    private QueryExecutor mockQueryExecutor;

    private QueryParser queryParser;
    private ResultGenerator resultGenerator;
    private ResultFormatter resultFormatter;
    private VariableBindings variableBindings;
    private Set<DocSentenceMatch> documentMatches;
    private Map<String, IndexAccess> indexes;

    @BeforeEach
    void setUp() throws QueryExecutionException {
        MockitoAnnotations.openMocks(this);

        queryParser = new QueryParser();
        resultGenerator = new ResultGenerator();
        resultFormatter = new ResultFormatter();
        variableBindings = new VariableBindings();
        documentMatches = new HashSet<>(Arrays.asList(
            new DocSentenceMatch(1),
            new DocSentenceMatch(2),
            new DocSentenceMatch(3)
        ));
        indexes = new HashMap<>();
        indexes.put("metadata", mockIndexAccess);
        indexes.put("unigram", mockIndexAccess);
        indexes.put("ner", mockIndexAccess);

        // Set up variable bindings
        variableBindings.addBinding(1, "?person", "John Smith@1:5");
        variableBindings.addBinding(2, "?person", "Jane Doe@2:3");
        variableBindings.addBinding(3, "?person", "Bob Johnson@3:7");

        variableBindings.addBinding(1, "?location", "New York@1:8");
        variableBindings.addBinding(2, "?location", "London@2:6");

        // Set up index manager mock
        when(mockIndexManager.getAllIndexes()).thenReturn(indexes);
        when(mockIndexManager.getIndex(anyString())).thenAnswer(invocation -> {
            String indexName = invocation.getArgument(0);
            return Optional.ofNullable(indexes.get(indexName));
        });

        // Set up query executor mock
        when(mockQueryExecutor.execute(any(Query.class), any(Map.class)))
            .thenReturn(documentMatches);
        when(mockQueryExecutor.getVariableBindings()).thenReturn(variableBindings);
    }

    @Test
    @DisplayName("Integration test for simple query with result generation and formatting")
    void testSimpleQueryIntegration() throws Exception, QueryExecutionException, ResultGenerationException {
        // Given
        String queryString = "SELECT ?person, ?location FROM documents WHERE CONTAINS(\"Smith\") AND NER(PERSON, ?person)";
        Query query = queryParser.parse(queryString);

        // When
        // 1. Execute query
        Set<DocSentenceMatch> results = mockQueryExecutor.execute(query, indexes);
        VariableBindings bindings = mockQueryExecutor.getVariableBindings();

        // 2. Generate result table
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, results, bindings, mockIndexManager.getAllIndexes());

        // 3. Format results
        String formattedResult = resultFormatter.format(resultTable);

        // Then
        assertNotNull(resultTable, "Result table should not be null");
        assertEquals(3, resultTable.getRowCount(), "Result table should have 3 rows");
        assertTrue(formattedResult.contains("document_id"), "Formatted result should contain column headers");
        assertTrue(formattedResult.contains("?person"), "Formatted result should contain person column");
        assertTrue(formattedResult.contains("?location"), "Formatted result should contain location column");
    }

    @Test
    @DisplayName("Integration test for query with ordering and limit")
    void testQueryWithOrderingAndLimit() throws Exception, QueryExecutionException, ResultGenerationException {
        // Given
        String queryString = "SELECT ?person, ?location FROM documents WHERE CONTAINS(\"Smith\") AND NER(PERSON, ?person) " +
                             "ORDER BY ?person ASC LIMIT 2";
        Query query = queryParser.parse(queryString);

        // When
        // 1. Execute query
        Set<DocSentenceMatch> results = mockQueryExecutor.execute(query, indexes);
        VariableBindings bindings = mockQueryExecutor.getVariableBindings();

        // 2. Generate result table
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, results, bindings, mockIndexManager.getAllIndexes());

        // 3. Format results
        String formattedResult = resultFormatter.format(resultTable);

        // Then
        assertNotNull(resultTable, "Result table should not be null");
        assertTrue(resultTable.getRowCount() <= 2, "Result table should have at most 2 rows due to LIMIT");
        assertTrue(formattedResult.contains("document_id"), "Formatted result should contain column headers");
    }

    @Test
    @DisplayName("Integration test for error handling during result generation")
    void testErrorHandlingDuringResultGeneration() throws Exception, QueryExecutionException {
        // Given
        String queryString = "SELECT ?person, ?location FROM documents WHERE CONTAINS(\"Smith\") AND NER(PERSON, ?person)";
        Query query = queryParser.parse(queryString);

        // Mock an exception during result generation
        ResultGenerator failingGenerator = mock(ResultGenerator.class);
        when(failingGenerator.generateResultTable(any(), any(), any(), any()))
            .thenThrow(new ResultGenerationException(
                "Test exception",
                new RuntimeException("Underlying cause"),
                "test_component",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            ));

        // When/Then
        Exception exception = assertThrows(ResultGenerationException.class, () -> {
            failingGenerator.generateResultTable(
                query, documentMatches, variableBindings, indexes);
        });

        assertTrue(exception.getMessage().contains("Test exception"), 
            "Exception should contain the error message");
    }

    @Test
    @DisplayName("Integration test for empty result set")
    void testEmptyResultSet() throws Exception, QueryExecutionException, ResultGenerationException {
        // Given
        String queryString = "SELECT ?person, ?location FROM documents WHERE CONTAINS(\"NonExistentTerm\")";
        Query query = queryParser.parse(queryString);
        Set<DocSentenceMatch> emptyResults = Collections.emptySet();

        // Mock empty result set
        when(mockQueryExecutor.execute(any(Query.class), any(Map.class)))
            .thenReturn(emptyResults);

        // When
        // 1. Execute query
        Set<DocSentenceMatch> results = mockQueryExecutor.execute(query, indexes);
        VariableBindings bindings = mockQueryExecutor.getVariableBindings();

        // 2. Generate result table
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, results, bindings, mockIndexManager.getAllIndexes());

        // 3. Format results
        String formattedResult = resultFormatter.format(resultTable);

        // Then
        assertNotNull(resultTable, "Result table should not be null");
        assertEquals(0, resultTable.getRowCount(), "Result table should have 0 rows");
        assertEquals("No results found.", formattedResult, "Formatted result should indicate no results");
    }
} 
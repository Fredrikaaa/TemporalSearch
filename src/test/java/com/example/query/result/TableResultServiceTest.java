package com.example.query.result;

import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.core.IndexAccessInterface;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import com.example.query.executor.QueryResult;
import com.example.query.model.Query;
import com.example.query.model.SelectColumn;
import com.example.query.model.VariableColumn;
import com.example.query.executor.SubqueryContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.tablesaw.api.*;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the TableResultService class.
 */
class TableResultServiceTest {
    
    private TableResultService tableResultService;
    private Map<String, IndexAccessInterface> indexes;
    
    @BeforeEach
    void setUp() {
        tableResultService = new TableResultService();
        indexes = new HashMap<>();
    }
    
    // Helper to create QueryResult
    private QueryResult createQueryResult(Query.Granularity granularity, List<MatchDetail> details) {
        // Assuming constructor QueryResult(granularity, details)
        return new QueryResult(granularity, details);
    }
    
    // Helper to create MatchDetail
    private MatchDetail createMatchDetail(int docId, int sentenceId, String value, ValueType type, String varName) {
        Position pos = new Position(docId, sentenceId, 0, 0, LocalDate.now());
        return new MatchDetail(value, type, pos, "mockCond", varName);
    }

    @Test
    void testGenerateTableDocumentGranularity() throws ResultGenerationException {
        Query query = new Query("testSource", Collections.emptyList(), Query.Granularity.DOCUMENT);
        List<MatchDetail> details = List.of(
            createMatchDetail(1, -1, "apple", ValueType.TERM, null),
            createMatchDetail(2, -1, "banana", ValueType.TERM, null)
        );
        QueryResult queryResult = createQueryResult(Query.Granularity.DOCUMENT, details);
        
        // Pass the QueryResult to generateTable
        Table table = tableResultService.generateTable(query, queryResult, indexes);
        
        assertNotNull(table);
        assertEquals(2, table.rowCount(), "Should have 2 rows for 2 documents");
        assertTrue(table.columnNames().contains("document_id"), "Should contain document_id column");
        // Assertions for document IDs (expecting IntColumn)
        assertEquals(1, table.intColumn("document_id").get(0), "Doc ID for first row");
        assertEquals(2, table.intColumn("document_id").get(1), "Doc ID for second row");
    }

    @Test
    void testGenerateTableSentenceGranularity() throws ResultGenerationException {
        Query query = new Query("testSource", Collections.emptyList(), Query.Granularity.SENTENCE);
         List<MatchDetail> details = List.of(
            createMatchDetail(1, 1, "apple", ValueType.TERM, null),
            createMatchDetail(1, 2, "banana", ValueType.TERM, null),
            createMatchDetail(2, 1, "cherry", ValueType.TERM, null)
        );
        QueryResult queryResult = createQueryResult(Query.Granularity.SENTENCE, details);

        Table table = tableResultService.generateTable(query, queryResult, indexes);
        
        assertNotNull(table);
        assertEquals(3, table.rowCount(), "Should have 3 rows for 3 sentences");
        assertTrue(table.columnNames().contains("document_id"), "Should contain document_id column");
        assertTrue(table.columnNames().contains("sentence_id"), "Should contain sentence_id column");
        // Assertions expecting IntColumns
        assertEquals(1, table.intColumn("document_id").get(0));
        assertEquals(1, table.intColumn("sentence_id").get(0));
        assertEquals(1, table.intColumn("document_id").get(1));
        assertEquals(2, table.intColumn("sentence_id").get(1));
        assertEquals(2, table.intColumn("document_id").get(2));
        assertEquals(1, table.intColumn("sentence_id").get(2));
    }
    
     @Test
    void testGenerateTableWithSelectColumns() throws ResultGenerationException {
        List<SelectColumn> select = List.of(new VariableColumn("?fruit"));
        Query query = new Query(
            "testSource",
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            select
        );
        
        List<MatchDetail> details = List.of(
            createMatchDetail(1, -1, "apple", ValueType.TERM, "?fruit"),
            createMatchDetail(2, -1, "banana", ValueType.TERM, "?fruit")
        );
        QueryResult queryResult = createQueryResult(Query.Granularity.DOCUMENT, details);

        Table table = tableResultService.generateTable(query, queryResult, indexes);

        assertNotNull(table);
        assertEquals(2, table.rowCount());
        assertTrue(table.columnNames().contains("document_id"));
        assertTrue(table.columnNames().contains("?fruit")); 
        assertEquals("apple", table.stringColumn("?fruit").get(0));
        assertEquals("banana", table.stringColumn("?fruit").get(1));
    }

    @Test
    void testGenerateTableEmptyResult() throws ResultGenerationException {
        Query query = new Query("testSource", Collections.emptyList(), Query.Granularity.DOCUMENT);
        QueryResult emptyResult = createQueryResult(Query.Granularity.DOCUMENT, Collections.emptyList());
        
        Table table = tableResultService.generateTable(query, emptyResult, indexes);
        
        assertNotNull(table);
        assertEquals(0, table.rowCount());
        assertTrue(table.name().contains("EmptyQueryResults"));
    }
} 
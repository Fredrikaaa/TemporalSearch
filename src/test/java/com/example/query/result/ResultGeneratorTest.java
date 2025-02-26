package com.example.query.result;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.model.OrderSpec;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the ResultGenerator class.
 */
class ResultGeneratorTest {
    
    private ResultGenerator resultGenerator;
    
    @Mock
    private Query query;
    
    @Mock
    private IndexAccess mockIndexAccess;
    
    private VariableBindings variableBindings;
    private Set<Integer> documentIds;
    private Map<String, IndexAccess> indexes;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        resultGenerator = new ResultGenerator();
        variableBindings = new VariableBindings();
        documentIds = new HashSet<>(Arrays.asList(1, 2, 3));
        indexes = new HashMap<>();
        indexes.put("metadata", mockIndexAccess);
        
        // Set up variable bindings
        variableBindings.addBinding(1, "?person", "John Smith@1:5");
        variableBindings.addBinding(2, "?person", "Jane Doe@2:3");
        variableBindings.addBinding(3, "?person", "Bob Johnson@3:7");
        
        variableBindings.addBinding(1, "?location", "New York@1:8");
        variableBindings.addBinding(2, "?location", "London@2:6");
        
        // Set up query mock
        when(query.getOrderBy()).thenReturn(Collections.emptyList());
        when(query.getLimit()).thenReturn(Optional.empty());
        when(query.getSource()).thenReturn("test_db");
    }
    
    @Test
    @DisplayName("Should generate a result table with correct columns and rows")
    void shouldGenerateResultTable() throws ResultGenerationException {
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentIds, variableBindings, indexes);
        
        // Then
        assertNotNull(resultTable, "Result table should not be null");
        
        // Verify columns
        List<ColumnSpec> columns = resultTable.getColumns();
        assertTrue(columns.size() >= 3, "Should have at least 3 columns");
        
        // Verify document_id column
        Optional<ColumnSpec> docIdColumn = columns.stream()
            .filter(c -> c.name().equals("document_id"))
            .findFirst();
        assertTrue(docIdColumn.isPresent(), "Should have document_id column");
        assertEquals(ColumnType.TERM, docIdColumn.get().type(), "document_id column should be TERM type");
        
        // Verify rows
        List<Map<String, String>> rows = resultTable.getRows();
        assertEquals(3, rows.size(), "Should have 3 rows");
        
        // Verify document IDs in rows
        Set<String> docIds = new HashSet<>();
        for (Map<String, String> row : rows) {
            docIds.add(row.get("document_id"));
        }
        assertEquals(Set.of("1", "2", "3"), docIds, "Rows should have correct document IDs");
        
        // Verify variable values in rows
        for (Map<String, String> row : rows) {
            String docId = row.get("document_id");
            if (docId.equals("1")) {
                assertEquals("John Smith@1:5", row.get("?person"), "Row 1 should have correct person value");
                assertEquals("New York@1:8", row.get("?location"), "Row 1 should have correct location value");
            } else if (docId.equals("2")) {
                assertEquals("Jane Doe@2:3", row.get("?person"), "Row 2 should have correct person value");
                assertEquals("London@2:6", row.get("?location"), "Row 2 should have correct location value");
            } else if (docId.equals("3")) {
                assertEquals("Bob Johnson@3:7", row.get("?person"), "Row 3 should have correct person value");
                assertNull(row.get("?location"), "Row 3 should not have location value");
            }
        }
    }
    
    @Test
    @DisplayName("Should apply ordering to result table")
    void shouldApplyOrdering() throws ResultGenerationException {
        // Given
        List<OrderSpec> orderSpecs = new ArrayList<>();
        orderSpecs.add(new OrderSpec("?person", OrderSpec.Direction.ASC));
        when(query.getOrderBy()).thenReturn(orderSpecs);
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentIds, variableBindings, indexes);
        
        // Then
        List<Map<String, String>> rows = resultTable.getRows();
        assertEquals(3, rows.size(), "Should have 3 rows");
        
        // Verify rows are ordered by person name
        assertEquals("3", rows.get(0).get("document_id"), "First row should be Bob Johnson");
        assertEquals("2", rows.get(1).get("document_id"), "Second row should be Jane Doe");
        assertEquals("1", rows.get(2).get("document_id"), "Third row should be John Smith");
    }
    
    @Test
    @DisplayName("Should apply limit to result table")
    void shouldApplyLimit() throws ResultGenerationException {
        // Given
        when(query.getLimit()).thenReturn(Optional.of(2));
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentIds, variableBindings, indexes);
        
        // Then
        List<Map<String, String>> rows = resultTable.getRows();
        assertEquals(2, rows.size(), "Should have 2 rows");
    }
    
    @Test
    @DisplayName("Should handle empty result set")
    void shouldHandleEmptyResultSet() throws ResultGenerationException {
        // Given
        Set<Integer> emptyDocumentIds = Collections.emptySet();
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, emptyDocumentIds, variableBindings, indexes);
        
        // Then
        assertNotNull(resultTable, "Result table should not be null");
        assertEquals(0, resultTable.getRowCount(), "Result table should have 0 rows");
    }
    
    @Test
    @DisplayName("Should handle exceptions during result generation")
    void shouldHandleExceptions() {
        // Given
        when(query.getSource()).thenThrow(new RuntimeException("Test exception"));
        
        // When/Then
        assertThrows(ResultGenerationException.class, () -> {
            resultGenerator.generateResultTable(query, documentIds, variableBindings, indexes);
        }, "Should throw ResultGenerationException");
    }
} 
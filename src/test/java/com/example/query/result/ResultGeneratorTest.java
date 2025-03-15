package com.example.query.result;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Ner;
import com.example.query.model.SelectColumn;
import com.example.query.model.VariableColumn;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.stream.Collectors;

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
    private Set<DocSentenceMatch> documentMatches;
    private Map<String, IndexAccess> indexes;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        resultGenerator = new ResultGenerator();
        variableBindings = new VariableBindings();
        documentMatches = new HashSet<>(Arrays.asList(
            new DocSentenceMatch(1),
            new DocSentenceMatch(2),
            new DocSentenceMatch(3)
        ));
        indexes = new HashMap<>();
        indexes.put("metadata", mockIndexAccess);
        
        // Set up variable bindings
        variableBindings.addBinding(1, "?person", "John Smith@1:5");
        variableBindings.addBinding(2, "?person", "Jane Doe@2:3");
        variableBindings.addBinding(3, "?person", "Bob Johnson@3:7");
        
        variableBindings.addBinding(1, "?location", "New York@1:8");
        variableBindings.addBinding(2, "?location", "London@2:6");
        
        // Set up query mock
        when(query.orderBy()).thenReturn(Collections.emptyList());
        when(query.limit()).thenReturn(Optional.empty());
        when(query.source()).thenReturn("test_db");
    }
    
    @Test
    @DisplayName("Should generate a result table with correct columns and rows")
    void shouldGenerateResultTable() throws ResultGenerationException {
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentMatches, variableBindings, indexes);
        
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
        // for (Map<String, String> row : rows) {
        //     String docId = row.get("document_id");
        //     if (docId.equals("1")) {
        //         assertEquals("John Smith", row.get("?person"), "Row 1 should have correct person value");
        //         assertEquals("New York", row.get("?location"), "Row 1 should have correct location value");
        //     } else if (docId.equals("2")) {
        //         assertEquals("Jane Doe", row.get("?person"), "Row 2 should have correct person value");
        //         assertEquals("London", row.get("?location"), "Row 2 should have correct location value");
        //     } else if (docId.equals("3")) {
        //         assertEquals("Bob Johnson", row.get("?person"), "Row 3 should have correct person value");
        //         assertNull(row.get("?location"), "Row 3 should not have location value");
        //     }
        // }
    }
    
    @Test
    @DisplayName("Should apply ordering to result table")
    void shouldApplyOrdering() throws ResultGenerationException {
        // Given
        List<String> orderColumns = new ArrayList<>();
        orderColumns.add("?person");
        when(query.orderBy()).thenReturn(orderColumns);
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentMatches, variableBindings, indexes);
        
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
        when(query.limit()).thenReturn(Optional.of(2));
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentMatches, variableBindings, indexes);
        
        // Then
        List<Map<String, String>> rows = resultTable.getRows();
        assertEquals(2, rows.size(), "Should have 2 rows");
    }
    
    @Test
    @DisplayName("Should handle empty result set")
    void shouldHandleEmptyResultSet() throws ResultGenerationException {
        // Given
        Set<DocSentenceMatch> emptyDocumentMatches = Collections.emptySet();
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, emptyDocumentMatches, variableBindings, indexes);
        
        // Then
        assertNotNull(resultTable, "Result table should not be null");
        assertEquals(0, resultTable.getRowCount(), "Result table should have 0 rows");
    }
    
    @Test
    @DisplayName("Should handle exceptions during result generation")
    void shouldHandleExceptions() {
        // Given
        when(query.selectColumns()).thenThrow(new RuntimeException("Test exception"));
        
        // When/Then
        assertThrows(ResultGenerationException.class, () -> {
            resultGenerator.generateResultTable(query, documentMatches, variableBindings, indexes);
        }, "Should throw ResultGenerationException");
    }
    
    @Test
    @DisplayName("Should create multiple rows when document has multiple entity matches")
    void shouldCreateMultipleRowsForMultipleEntities() throws ResultGenerationException {
        // Set up
        VariableBindings multiEntityBindings = new VariableBindings();
        
        // Document 10 has three organization entities
        multiEntityBindings.addBinding(10, "entity", "yale@100:104");
        multiEntityBindings.addBinding(10, "entity", "princeton@200:208");
        multiEntityBindings.addBinding(10, "entity", "rutgers@300:307");
        
        Set<DocSentenceMatch> matches = new HashSet<>();
        matches.add(new DocSentenceMatch(10));
        
        // Set up query mock for a variable in SELECT
        List<SelectColumn> selectColumns = new ArrayList<>();
        SelectColumn entityColumn = mock(SelectColumn.class);
        when(entityColumn.toString()).thenReturn("?entity");
        selectColumns.add(entityColumn);
        when(query.selectColumns()).thenReturn(selectColumns);
        
        // Ensure required methods return expected values
        when(query.granularity()).thenReturn(Query.Granularity.DOCUMENT);
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, matches, multiEntityBindings, indexes);
        
        // Then
        assertNotNull(resultTable);
        
        // Should have three rows, one for each entity in the document
        assertEquals(3, resultTable.getRows().size(), "Should have 3 rows, one for each entity in document 10");
        
        // Verify all rows have document_id = 10
        for (Map<String, String> row : resultTable.getRows()) {
            assertEquals("10", row.get("document_id"), "All rows should have document_id = 10");
        }
        
        // Check that each distinct entity value is present in exactly one row
        Set<String> entityValues = resultTable.getRows().stream()
            .map(row -> row.get("entity"))
            .collect(Collectors.toSet());
        
        assertEquals(3, entityValues.size(), "Should have 3 distinct entity values");
        assertTrue(entityValues.contains("yale@100:104"), "Should have 'yale' entity");
        assertTrue(entityValues.contains("princeton@200:208"), "Should have 'princeton' entity");
        assertTrue(entityValues.contains("rutgers@300:307"), "Should have 'rutgers' entity");
    }
    
    @Test
    public void testCorrectSentenceIdForDifferentTextLocations() throws Exception {
        // Create test document with 3 instances of "Joe Biden" in different sentences
        int documentId = 10;
        int sentence1Id = 3;
        int sentence2Id = 42;
        int sentence3Id = 45;
        
        // Create matches for each sentence
        Set<DocSentenceMatch> matches = new HashSet<>();
        DocSentenceMatch match1 = new DocSentenceMatch(documentId, sentence1Id);
        DocSentenceMatch match2 = new DocSentenceMatch(documentId, sentence2Id);
        DocSentenceMatch match3 = new DocSentenceMatch(documentId, sentence3Id);
        matches.add(match1);
        matches.add(match2);
        matches.add(match3);
        
        // Setup variable bindings for each sentence
        VariableBindings sentenceBindings = new VariableBindings();
        sentenceBindings.addBinding(documentId, sentence1Id, "?person", "Joe Biden@10:19");
        sentenceBindings.addBinding(documentId, sentence2Id, "?person", "Joe Biden@100:109");
        sentenceBindings.addBinding(documentId, sentence3Id, "?person", "Joe Biden@200:209");
        
        // Set up query mock
        when(query.granularity()).thenReturn(Query.Granularity.SENTENCE);
        when(query.limit()).thenReturn(Optional.empty());
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, matches, sentenceBindings, indexes);
        
        // Then
        assertNotNull(resultTable);
        assertEquals(3, resultTable.getRows().size(), "Should have 3 rows");
        
        // Verify each row has correct sentence ID and text location
        for (Map<String, String> row : resultTable.getRows()) {
            String sentenceId = row.get("sentence_id");
            String personValue = row.get("?person");
            
            if (sentenceId.equals(String.valueOf(sentence1Id))) {
                assertEquals("Joe Biden@10:19", personValue);
            } else if (sentenceId.equals(String.valueOf(sentence2Id))) {
                assertEquals("Joe Biden@100:109", personValue);
            } else if (sentenceId.equals(String.valueOf(sentence3Id))) {
                assertEquals("Joe Biden@200:209", personValue);
            } else {
                fail("Unexpected sentence ID: " + sentenceId);
            }
        }
    }
    
    @Test
    @DisplayName("Should include NER variable values in result rows")
    public void testNerVariablesAppearInResultRows() throws ResultGenerationException {
        // Given
        List<Condition> conditions = new ArrayList<>();
        conditions.add(Ner.withVariable("PERSON", "?person"));
        when(query.conditions()).thenReturn(conditions);
        
        // Set up query mock for granularity and select columns
        when(query.granularity()).thenReturn(Query.Granularity.DOCUMENT);
        when(query.limit()).thenReturn(Optional.empty());
        when(query.selectColumns()).thenReturn(Arrays.asList(
            new VariableColumn("?person")
        ));
        
        // When
        ResultTable resultTable = resultGenerator.generateResultTable(
            query, documentMatches, variableBindings, indexes);
        
        // Then
        assertNotNull(resultTable);
        List<Map<String, String>> rows = resultTable.getRows();
        assertEquals(3, rows.size(), "Should have 3 rows");
        
        // Verify person values in rows
        for (Map<String, String> row : rows) {
            String docId = row.get("document_id");
            String personValue = row.get("?person");
            assertNotNull(personValue, "Person value should not be null");
            
            switch (docId) {
                case "1":
                    assertEquals("John Smith@1:5", personValue);
                    break;
                case "2":
                    assertEquals("Jane Doe@2:3", personValue);
                    break;
                case "3":
                    assertEquals("Bob Johnson@3:7", personValue);
                    break;
                default:
                    fail("Unexpected document ID: " + docId);
            }
        }
    }
} 
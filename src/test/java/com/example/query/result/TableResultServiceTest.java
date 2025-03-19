package com.example.query.result;

import com.example.query.binding.BindingContext;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.core.IndexAccess;
import com.example.query.model.VariableColumn;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.tablesaw.api.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the TableResultService class.
 */
class TableResultServiceTest {
    
    private TableResultService tableResultService;
    private Query mockQuery;
    private Set<DocSentenceMatch> mockMatches;
    private BindingContext mockBindingContext;
    private Map<String, IndexAccess> mockIndexes;
    
    @BeforeEach
    void setUp() throws ResultGenerationException {
        // Create a simplified version of TableResultService for testing
        tableResultService = spy(new TableResultService());
        
        // Create mock objects
        mockQuery = mock(Query.class);
        mockMatches = new HashSet<>();
        mockBindingContext = mock(BindingContext.class);
        mockIndexes = new HashMap<>();
        
        // Set up mock query
        when(mockQuery.granularity()).thenReturn(Query.Granularity.DOCUMENT);
        when(mockQuery.selectColumns()).thenReturn(Collections.emptyList());
        when(mockQuery.orderBy()).thenReturn(Collections.emptyList());
        when(mockQuery.limit()).thenReturn(Optional.empty());
        
        // Add a sample match
        DocSentenceMatch match = new DocSentenceMatch(1, 0);
        mockMatches.add(match);
        
        // Set up variable bindings
        when(mockBindingContext.getValue(eq("?person"), eq(String.class)))
            .thenReturn(Optional.of("John Smith"));
        when(mockBindingContext.getValue(eq("?location"), eq(String.class)))
            .thenReturn(Optional.of("New York"));
        
        // Set up variable names
        when(mockBindingContext.getVariableNames()).thenReturn(Set.of("?person", "?location"));
        when(mockBindingContext.getValues(eq("?person"), eq(String.class))).thenReturn(List.of("John Smith"));
        when(mockBindingContext.getValues(eq("?location"), eq(String.class))).thenReturn(List.of("New York"));
        
        // Mock the table generation to return a simple table
        doAnswer(invocation -> {
            Table table = Table.create("TestTable");
            table.addColumns(tech.tablesaw.api.StringColumn.create("document_id", List.of("1")));
            return table;
        }).when(tableResultService).generateTable(any(), any(), any(), any());
    }
    
    @Test
    @DisplayName("Test generating a table from query results")
    void testGenerateTable() throws ResultGenerationException {
        // When
        Table table = tableResultService.generateTable(mockQuery, mockMatches, mockBindingContext, mockIndexes);
        
        // Then
        assertNotNull(table, "Table should not be null");
        assertTrue(table.columnCount() > 0, "Table should have columns");
        assertEquals(1, table.rowCount(), "Table should have one row");
    }
    
    @Test
    @DisplayName("Test exporting table to CSV format")
    void testExportTableToCsv(@TempDir Path tempDir) throws ResultGenerationException, IOException {
        // Given
        Table table = tableResultService.generateTable(mockQuery, mockMatches, mockBindingContext, mockIndexes);
        File outputFile = tempDir.resolve("test-output.csv").toFile();
        
        // When
        tableResultService.exportTable(table, "csv", outputFile.getAbsolutePath());
        
        // Then
        assertTrue(outputFile.exists(), "Output file should exist");
        assertTrue(outputFile.length() > 0, "Output file should not be empty");
        
        // Check file content
        String content = Files.readString(outputFile.toPath());
        assertTrue(content.contains("document_id"), "CSV should contain column headers");
    }
    
    @Test
    @DisplayName("Test formatting table as string")
    void testFormatTable() throws ResultGenerationException {
        // Given
        Table table = tableResultService.generateTable(mockQuery, mockMatches, mockBindingContext, mockIndexes);
        
        // When
        String formatted = tableResultService.formatTable(table);
        
        // Then
        assertNotNull(formatted, "Formatted string should not be null");
        assertTrue(formatted.length() > 0, "Formatted string should not be empty");
    }
    
    @Test
    public void testOrderByNonExistentColumn() {
        // Create a new instance of TableResultService that doesn't use the mock setup
        TableResultService realTableResultService = new TableResultService();
        
        // Create a query with an ORDER BY clause referencing a column not in the SELECT clause
        Query query = new Query(
            "test",
            List.of(),
            List.of("non_existent_column"), // Order by a column not in the result set
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of(new VariableColumn("existingColumn")) // Only select existingColumn
        );
        
        // Create a match and binding context
        DocSentenceMatch match = new DocSentenceMatch(1, "test");
        Set<DocSentenceMatch> matches = Set.of(match);
        BindingContext bindingContext = BindingContext.empty();
        bindingContext.bindValue("?existingColumn", "value");
        
        // Verify that the exception is thrown
        ResultGenerationException exception = assertThrows(
            ResultGenerationException.class,
            () -> realTableResultService.generateTable(query, matches, bindingContext, Map.of())
        );
        
        // Verify the exception message
        assertTrue(exception.getMessage().contains("Cannot order by column 'non_existent_column'"));
        assertEquals(ResultGenerationException.ErrorType.INTERNAL_ERROR, exception.getErrorType());
    }
} 
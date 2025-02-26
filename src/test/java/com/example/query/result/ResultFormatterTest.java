package com.example.query.result;

import com.example.query.model.ResultTable;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the ResultFormatter class.
 */
class ResultFormatterTest {
    
    private ResultFormatter formatter;
    private ResultTable resultTable;
    
    @BeforeEach
    void setUp() {
        formatter = new ResultFormatter();
        
        // Create a sample result table
        List<ColumnSpec> columns = new ArrayList<>();
        columns.add(new ColumnSpec("document_id", ColumnType.TERM));
        columns.add(new ColumnSpec("person", ColumnType.PERSON));
        columns.add(new ColumnSpec("location", ColumnType.LOCATION));
        
        List<Map<String, String>> rows = new ArrayList<>();
        
        Map<String, String> row1 = new HashMap<>();
        row1.put("document_id", "1");
        row1.put("person", "John Smith");
        row1.put("location", "New York");
        
        Map<String, String> row2 = new HashMap<>();
        row2.put("document_id", "2");
        row2.put("person", "Jane Doe");
        row2.put("location", "London");
        
        Map<String, String> row3 = new HashMap<>();
        row3.put("document_id", "3");
        row3.put("person", "Bob Johnson");
        row3.put("location", null);
        
        rows.add(row1);
        rows.add(row2);
        rows.add(row3);
        
        resultTable = new ResultTable(columns, rows);
    }
    
    @Test
    @DisplayName("Should format result table with default options")
    void shouldFormatResultTableWithDefaultOptions() {
        // When
        String formatted = formatter.format(resultTable);
        
        // Then
        assertNotNull(formatted, "Formatted output should not be null");
        assertTrue(formatted.contains("document_id"), "Output should contain document_id header");
        assertTrue(formatted.contains("person"), "Output should contain person header");
        assertTrue(formatted.contains("location"), "Output should contain location header");
        assertTrue(formatted.contains("John Smith"), "Output should contain John Smith");
        assertTrue(formatted.contains("Jane Doe"), "Output should contain Jane Doe");
        assertTrue(formatted.contains("Bob Johnson"), "Output should contain Bob Johnson");
        assertTrue(formatted.contains("New York"), "Output should contain New York");
        assertTrue(formatted.contains("London"), "Output should contain London");
    }
    
    @Test
    @DisplayName("Should format result table with custom options")
    void shouldFormatResultTableWithCustomOptions() {
        // Given
        ResultFormatter.FormattingOptions options = new ResultFormatter.FormattingOptions(
            true,   // showHeaders
            true,   // showRowNumbers
            10,     // maxColumnWidth
            true    // truncateLongValues
        );
        
        // When
        String formatted = formatter.format(resultTable, options);
        
        // Then
        assertNotNull(formatted, "Formatted output should not be null");
        assertTrue(formatted.contains("#"), "Output should contain row number header");
        
        // Check for truncation of long values
        if (formatted.contains("Bob Johnso")) {
            // If truncated to 10 chars
            assertFalse(formatted.contains("Bob Johnson"), "Long name should be truncated");
        }
    }
    
    @Test
    @DisplayName("Should handle empty result table")
    void shouldHandleEmptyResultTable() {
        // Given
        ResultTable emptyTable = new ResultTable(new ArrayList<>(), new ArrayList<>());
        
        // When
        String formatted = formatter.format(emptyTable);
        
        // Then
        assertEquals("No results found.", formatted, "Should show 'No results found' message");
    }
    
    @Test
    @DisplayName("Should handle result table with no columns")
    void shouldHandleResultTableWithNoColumns() {
        // Given
        ResultTable noColumnsTable = new ResultTable(
            new ArrayList<>(),
            resultTable.getRows()
        );
        
        // When
        String formatted = formatter.format(noColumnsTable);
        
        // Then
        assertNotNull(formatted, "Formatted output should not be null");
        assertFalse(formatted.isEmpty(), "Formatted output should not be empty");
    }
    
    @Test
    @DisplayName("Should handle result table with no rows")
    void shouldHandleResultTableWithNoRows() {
        // Given
        ResultTable noRowsTable = new ResultTable(
            resultTable.getColumns(),
            new ArrayList<>()
        );
        
        // When
        String formatted = formatter.format(noRowsTable);
        
        // Then
        assertEquals("No results found.", formatted, "Should show 'No results found' message");
    }
} 
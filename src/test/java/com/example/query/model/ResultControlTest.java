package com.example.query.model;

import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.query.format.TableConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the result control features (ORDER BY, LIMIT, COUNT).
 */
public class ResultControlTest {

    private ResultTable sampleTable;

    @BeforeEach
    void setUp() {
        // Create a sample result table with test data
        List<ColumnSpec> columns = List.of(
            new ColumnSpec("person", ColumnType.PERSON),
            new ColumnSpec("org", ColumnType.TERM),
            new ColumnSpec("date", ColumnType.DATE)
        );
        
        List<Map<String, String>> rows = new ArrayList<>();
        
        // Add sample data in unsorted order
        rows.add(createRow("John Smith", "Google", "2023-01-15"));
        rows.add(createRow("Alice Johnson", "Microsoft", "2023-03-20"));
        rows.add(createRow("Bob Brown", "Apple", "2023-02-10"));
        rows.add(createRow("Zack Wilson", "Netflix", "2023-01-05"));
        rows.add(createRow("Eve Davis", "Amazon", "2023-04-30"));
        
        sampleTable = new ResultTable(columns, rows, 10, TableConfig.getDefault());
    }
    
    private Map<String, String> createRow(String person, String org, String date) {
        Map<String, String> row = new HashMap<>();
        row.put("person", person);
        row.put("org", org);
        row.put("date", date);
        return row;
    }
    
    @Test
    void testOrderByAscending() {
        // Order by person name ascending
        List<String> orderColumns = List.of("person");
        
        ResultTable sortedTable = sampleTable.sort(orderColumns);
        
        // Verify the order is correct
        List<String> personColumn = extractColumn(sortedTable, "person");
        assertTrue(isSorted(personColumn, true));
        assertEquals("Alice Johnson", personColumn.get(0));
        assertEquals("Zack Wilson", personColumn.get(4));
    }
    
    @Test
    void testOrderByDescending() {
        // Order by organization name descending
        List<String> orderColumns = List.of("-org");
        
        ResultTable sortedTable = sampleTable.sort(orderColumns);
        
        // Verify the order is correct
        List<String> orgColumn = extractColumn(sortedTable, "org");
        assertTrue(isSorted(orgColumn, false));
        assertEquals("Netflix", orgColumn.get(0));
        assertEquals("Amazon", orgColumn.get(4));
    }
    
    @Test
    void testMultipleOrderBy() {
        // Create a table with duplicate org values
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("John Smith", "Google", "2023-01-15"));
        rows.add(createRow("Alice Johnson", "Microsoft", "2023-03-20"));
        rows.add(createRow("Bob Brown", "Google", "2023-02-10"));
        rows.add(createRow("Zack Wilson", "Microsoft", "2023-01-05"));
        
        ResultTable table = new ResultTable(sampleTable.getColumns(), rows, 10, TableConfig.getDefault());
        
        // Order by org ascending, then by person descending
        List<String> orderColumns = List.of("org", "-person");
        
        ResultTable sortedTable = table.sort(orderColumns);
        
        // Verify the order is correct
        List<String> orgColumn = extractColumn(sortedTable, "org");
        List<String> personColumn = extractColumn(sortedTable, "person");
        
        assertEquals("Google", orgColumn.get(0));
        assertEquals("Google", orgColumn.get(1));
        assertEquals("John Smith", personColumn.get(0)); // Google, John comes after Bob alphabetically but DESC
        assertEquals("Bob Brown", personColumn.get(1));  // Google
        assertEquals("Microsoft", orgColumn.get(2));
        assertEquals("Microsoft", orgColumn.get(3));
    }
    
    @Test
    void testLimit() {
        // Apply a limit of 3 rows
        ResultTable limitedTable = sampleTable.limit(3);
        
        // Verify the row count is limited
        assertEquals(3, limitedTable.getRowCount());
        assertEquals(sampleTable.getValue(0, "person"), limitedTable.getValue(0, "person"));
        assertEquals(sampleTable.getValue(1, "person"), limitedTable.getValue(1, "person"));
        assertEquals(sampleTable.getValue(2, "person"), limitedTable.getValue(2, "person"));
    }
    
    @Test
    void testLimitExceedingRowCount() {
        // Apply a limit larger than the row count
        ResultTable limitedTable = sampleTable.limit(10);
        
        // Verify all rows are returned
        assertEquals(sampleTable.getRowCount(), limitedTable.getRowCount());
    }
    
    @Test
    void testCountAll() {
        // Count all rows
        int count = sampleTable.countAll();
        
        // Verify the count is correct
        assertEquals(5, count);
    }
    
    @Test
    void testCountUnique() {
        // Create a table with duplicate org values
        List<Map<String, String>> rows = new ArrayList<>();
        rows.add(createRow("John Smith", "Google", "2023-01-15"));
        rows.add(createRow("Alice Johnson", "Microsoft", "2023-03-20"));
        rows.add(createRow("Bob Brown", "Google", "2023-02-10"));
        rows.add(createRow("Zack Wilson", "Microsoft", "2023-01-05"));
        
        ResultTable table = new ResultTable(sampleTable.getColumns(), rows, 10, TableConfig.getDefault());
        
        // Count unique org values
        int count = table.countUnique("org");
        
        // Verify the count is correct
        assertEquals(2, count);
    }
    
    @Test
    void testOrderByWithLimit() {
        // Order by person name ascending and limit to 2 rows
        List<String> orderColumns = List.of("person");
        
        ResultTable sortedTable = sampleTable.sort(orderColumns);
        ResultTable limitedTable = sortedTable.limit(2);
        
        // Verify the order and limit are correct
        List<String> personColumn = extractColumn(limitedTable, "person");
        assertEquals(2, limitedTable.getRowCount());
        assertEquals("Alice Johnson", personColumn.get(0));
        assertEquals("Bob Brown", personColumn.get(1));
    }
    
    // Helper methods
    
    private List<String> extractColumn(ResultTable table, String columnName) {
        List<String> column = new ArrayList<>();
        for (int i = 0; i < table.getRowCount(); i++) {
            column.add(table.getValue(i, columnName));
        }
        return column;
    }
    
    private boolean isSorted(List<String> list, boolean ascending) {
        if (list.size() <= 1) {
            return true;
        }
        
        List<String> sorted = new ArrayList<>(list);
        if (ascending) {
            Collections.sort(sorted);
        } else {
            sorted.sort(Collections.reverseOrder());
        }
        
        return sorted.equals(list);
    }
} 
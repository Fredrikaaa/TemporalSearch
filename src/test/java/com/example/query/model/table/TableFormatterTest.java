package com.example.query.model.table;

import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TableFormatterTest {
    private TableFormatter formatter;
    private ResultTable table;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        // Redirect System.out for testing console output
        System.setOut(new PrintStream(outContent));
        
        // Create test data
        List<ColumnSpec> columns = Arrays.asList(
            new ColumnSpec("PERSON", ColumnType.PERSON),
            new ColumnSpec("DATE", ColumnType.DATE),
            new ColumnSpec("SNIPPET", ColumnType.SNIPPET)
        );
        
        List<Map<String, String>> rows = Arrays.asList(
            createRow("John Doe", "2024-01-28", "This is a test snippet"),
            createRow("Jane Smith", "2024-01-29", "Another test snippet"),
            createRow("Bob Wilson", "2024-01-30", "Final test snippet")
        );
        
        table = ResultTable.create(columns, rows);
        formatter = new TableFormatter(new TableConfig(80, 2, null, true));
    }
    
    @Test
    void testDisplayPreview() {
        formatter.displayPreview(table);
        
        String output = outContent.toString();
        assertTrue(output.contains("PERSON"));
        assertTrue(output.contains("DATE"));
        assertTrue(output.contains("SNIPPET"));
        assertTrue(output.contains("John Doe"));
        assertTrue(output.contains("2024-01-28"));
        assertTrue(output.contains("This is a test snippet"));
        assertTrue(output.contains("Showing 2 of 3 rows"));
    }
    
    @Test
    void testWriteCSV() throws Exception {
        Path csvFile = tempDir.resolve("test.csv");
        formatter.writeCSV(table, csvFile.toString());
        
        List<String> lines = Files.readAllLines(csvFile);
        assertEquals(4, lines.size()); // Header + 3 data rows
        assertTrue(lines.get(0).contains("PERSON,DATE,SNIPPET"));
        assertTrue(lines.get(1).contains("John Doe,2024-01-28"));
    }
    
    @Test
    void testLongValues() {
        String longSnippet = "This is a very long snippet that should be truncated " +
            "because it exceeds the maximum column width defined in the formatter";
        
        List<Map<String, String>> rows = Arrays.asList(
            createRow("John Doe", "2024-01-28", longSnippet)
        );
        
        ResultTable longTable = ResultTable.create(table.columns(), rows);
        formatter.displayPreview(longTable);
        
        String output = outContent.toString();
        assertTrue(output.contains("..."), "Output should contain truncation indicator");
        
        // Extract the snippet column from the output
        String[] lines = output.split("\n");
        // Find the data line (it contains "John Doe")
        String dataLine = null;
        for (String line : lines) {
            if (line.contains("John Doe")) {
                dataLine = line;
                break;
            }
        }
        assertNotNull(dataLine, "Data line should be found");
        
        // Extract the snippet cell (everything after the last |)
        String snippetCell = dataLine.substring(dataLine.lastIndexOf("|") + 1).trim();
        assertTrue(snippetCell.length() <= TableFormatter.MAX_COLUMN_WIDTH + 3,
            String.format("Snippet length %d should be <= max width %d + 3",
                snippetCell.length(), TableFormatter.MAX_COLUMN_WIDTH));
    }
    
    @Test
    void testCSVEscaping() throws Exception {
        String valueWithComma = "Doe, John";
        String valueWithQuotes = "\"quoted\"";
        
        List<Map<String, String>> rows = Arrays.asList(
            createRow(valueWithComma, valueWithQuotes, "Normal value")
        );
        
        ResultTable escapeTable = ResultTable.create(table.columns(), rows);
        Path csvFile = tempDir.resolve("escape_test.csv");
        formatter.writeCSV(escapeTable, csvFile.toString());
        
        List<String> lines = Files.readAllLines(csvFile);
        String dataLine = lines.get(1);
        assertTrue(dataLine.contains("\"Doe, John\""));  // Should be quoted
        assertTrue(dataLine.contains("\"\"\"quoted\"\"\""));  // Should escape quotes
    }
    
    private Map<String, String> createRow(String person, String date, String snippet) {
        Map<String, String> row = new HashMap<>();
        row.put("PERSON", person);
        row.put("DATE", date);
        row.put("SNIPPET", snippet);
        return row;
    }
    
    @org.junit.jupiter.api.AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
    }
} 
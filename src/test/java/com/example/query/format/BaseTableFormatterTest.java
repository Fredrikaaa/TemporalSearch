package com.example.query.format;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

class BaseTableFormatterTest {
    private TableFormatter formatter;
    private List<ColumnFormat> columnFormats;
    private List<String> headers;
    private List<List<TableCell>> rows;
    private TableConfig config;

    @BeforeEach
    void setUp() {
        columnFormats = Arrays.asList(
            ColumnFormat.createDefault("person", 10),
            ColumnFormat.createDefault("date", 10),
            new ColumnFormat("snippet", "Context", 20, ColumnFormat.TextAlign.LEFT, String::trim)
        );
        
        formatter = new BaseTableFormatter(columnFormats);
        
        headers = Arrays.asList("PERSON", "DATE", "SNIPPET");
        
        rows = Arrays.asList(
            Arrays.asList(
                TableCell.withHighlights("Obama", Arrays.asList("*Obama*")),
                TableCell.of("2012"),
                TableCell.of("Obama was president")
            ),
            Arrays.asList(
                TableCell.withHighlights("Bush", Arrays.asList("*Bush*")),
                TableCell.of("2004"),
                TableCell.of("Bush won re-election")
            )
        );
        
        config = new TableConfig(10, "test.csv", true, 50, ',');
    }

    @Test
    void testFormatPreviewWithHeaders() {
        String preview = formatter.formatPreview(headers, rows, config);
        
        // Verify basic structure
        assertTrue(preview.contains("PERSON"));
        assertTrue(preview.contains("DATE"));
        assertTrue(preview.contains("SNIPPET"));
        assertTrue(preview.contains("Obama"));
        assertTrue(preview.contains("2012"));
        
        // Verify separator line
        assertTrue(preview.contains("+-"));
        
        // Verify proper alignment
        assertTrue(preview.contains("| Obama"));
        assertTrue(preview.contains("| 2012"));
    }

    @Test
    void testFormatPreviewWithoutHeaders() {
        config = new TableConfig(10, "test.csv", false, 50, ',');
        String preview = formatter.formatPreview(headers, rows, config);
        
        // Verify headers are not present
        assertFalse(preview.contains("+-"));
        assertTrue(preview.contains("Obama"));
    }

    @Test
    void testFormatPreviewWithEmptyRows() {
        String preview = formatter.formatPreview(headers, List.of(), config);
        assertEquals("No results to display", preview);
    }

    @Test
    void testFormatPreviewWithRowLimit() {
        config = new TableConfig(1, "test.csv", true, 50, ',');
        String preview = formatter.formatPreview(headers, rows, config);
        
        // Verify only first row is shown
        assertTrue(preview.contains("Obama"));
        assertFalse(preview.contains("Bush"));
        assertTrue(preview.contains("(1 more rows, saved to test.csv)"));
    }

    @Test
    void testWriteCSV(@TempDir Path tempDir) throws IOException {
        Path csvFile = tempDir.resolve("test.csv");
        config = new TableConfig(10, csvFile.toString(), true, 50, ',');
        
        formatter.writeCSV(headers, rows, config);
        
        List<String> lines = Files.readAllLines(csvFile);
        assertEquals(3, lines.size()); // Headers + 2 data rows
        
        // Verify headers
        assertEquals("PERSON,DATE,SNIPPET", lines.get(0));
        
        // Verify data rows
        assertEquals("Obama,2012,Obama was president", lines.get(1));
        assertEquals("Bush,2004,Bush won re-election", lines.get(2));
    }

    @Test
    void testWriteCSVWithoutHeaders(@TempDir Path tempDir) throws IOException {
        Path csvFile = tempDir.resolve("test.csv");
        config = new TableConfig(10, csvFile.toString(), false, 50, ',');
        
        formatter.writeCSV(headers, rows, config);
        
        List<String> lines = Files.readAllLines(csvFile);
        assertEquals(2, lines.size()); // Only data rows
        
        // Verify data rows directly
        assertEquals("Obama,2012,Obama was president", lines.get(0));
        assertEquals("Bush,2004,Bush won re-election", lines.get(1));
    }

    @Test
    void testWriteCSVWithSpecialCharacters(@TempDir Path tempDir) throws IOException {
        Path csvFile = tempDir.resolve("test.csv");
        config = new TableConfig(10, csvFile.toString(), true, 50, ',');
        
        List<List<TableCell>> specialRows = Arrays.asList(
            Arrays.asList(
                TableCell.of("O'Brien"),
                TableCell.of("2012"),
                TableCell.of("Contains, comma")
            ),
            Arrays.asList(
                TableCell.of("Smith"),
                TableCell.of("2004"),
                TableCell.of("Contains \"quotes\"")
            )
        );
        
        formatter.writeCSV(headers, specialRows, config);
        
        List<String> lines = Files.readAllLines(csvFile);
        assertEquals(3, lines.size());
        
        // Verify special characters are properly escaped
        assertEquals("O'Brien,2012,\"Contains, comma\"", lines.get(1));
        assertEquals("Smith,2004,\"Contains \"\"quotes\"\"\"", lines.get(2));
    }

    @Test
    void testGetColumnFormats() {
        List<ColumnFormat> formats = formatter.getColumnFormats();
        
        // Verify we get a copy of the formats
        assertEquals(columnFormats, formats);
        assertNotSame(columnFormats, formats);
        
        // Verify modifying the returned list doesn't affect the formatter
        formats.clear();
        assertEquals(columnFormats, formatter.getColumnFormats());
    }
} 
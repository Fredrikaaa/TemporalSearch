package com.example.query.model.table;

import com.example.query.model.column.ColumnSpec;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Handles formatting and output of query results in table format.
 */
public class TableFormatter {
    private static final Logger logger = Logger.getLogger(TableFormatter.class.getName());
    private static final String CSV_SEPARATOR = ",";
    private static final int MIN_COLUMN_WIDTH = 10;
    public static final int MAX_COLUMN_WIDTH = 50;
    
    private final TableConfig config;
    
    public TableFormatter(TableConfig config) {
        this.config = config;
    }
    
    /**
     * Displays a preview of the result table in the console.
     * @param table The result table to display
     */
    public void displayPreview(ResultTable table) {
        List<Integer> columnWidths = calculateColumnWidths(table);
        
        // Print headers
        if (config.showHeaders()) {
            printRowSeparator(columnWidths);
            printHeaderRow(table.columns(), columnWidths);
            printRowSeparator(columnWidths);
        }
        
        // Print rows
        int rowCount = 0;
        for (Map<String, String> row : table.rows()) {
            if (rowCount >= config.previewRows()) {
                break;
            }
            printDataRow(row, table.columns(), columnWidths);
            rowCount++;
        }
        
        // Print footer
        if (config.showHeaders()) {
            printRowSeparator(columnWidths);
        }
        
        // Print total count if there are more rows
        if (table.rows().size() > config.previewRows()) {
            System.out.printf("\nShowing %d of %d rows\n", 
                config.previewRows(), table.rows().size());
        }
    }
    
    /**
     * Writes the result table to a CSV file.
     * @param table The result table to write
     * @param outputFile The path to the output file
     * @throws IOException If writing fails
     */
    public void writeCSV(ResultTable table, String outputFile) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            // Write headers
            if (config.showHeaders()) {
                writer.write(String.join(CSV_SEPARATOR, 
                    table.columns().stream()
                        .map(ColumnSpec::name)
                        .map(this::escapeCSV)
                        .toList()
                ));
                writer.newLine();
            }
            
            // Write data rows
            for (Map<String, String> row : table.rows()) {
                writer.write(String.join(CSV_SEPARATOR,
                    table.columns().stream()
                        .map(col -> escapeCSV(row.getOrDefault(col.name(), "")))
                        .toList()
                ));
                writer.newLine();
            }
        }
    }
    
    private List<Integer> calculateColumnWidths(ResultTable table) {
        List<Integer> widths = new ArrayList<>();
        
        for (ColumnSpec col : table.columns()) {
            // Start with header width
            int maxWidth = Math.min(col.name().length(), MAX_COLUMN_WIDTH);
            
            // Check data widths
            for (Map<String, String> row : table.rows()) {
                String value = row.getOrDefault(col.name(), "");
                // Account for truncation marker "..."
                int valueWidth = Math.min(value.length(), MAX_COLUMN_WIDTH);
                maxWidth = Math.max(maxWidth, Math.min(valueWidth, MAX_COLUMN_WIDTH));
            }
            
            // Apply min/max constraints
            maxWidth = Math.max(MIN_COLUMN_WIDTH, maxWidth);
            widths.add(maxWidth);
        }
        
        return widths;
    }
    
    private void printRowSeparator(List<Integer> columnWidths) {
        columnWidths.forEach(width -> 
            System.out.print("+" + "-".repeat(width + 2)));
        System.out.println("+");
    }
    
    private void printHeaderRow(List<ColumnSpec> columns, List<Integer> columnWidths) {
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            int width = columnWidths.get(i);
            System.out.printf("| %-" + width + "s ", name);
        }
        System.out.println("|");
    }
    
    private void printDataRow(Map<String, String> row, List<ColumnSpec> columns, 
            List<Integer> columnWidths) {
        for (int i = 0; i < columns.size(); i++) {
            String value = row.getOrDefault(columns.get(i).name(), "");
            int width = columnWidths.get(i);
            
            // Truncate if necessary, ensuring we don't exceed MAX_COLUMN_WIDTH
            if (value.length() > width) {
                int truncateWidth = Math.min(width, MAX_COLUMN_WIDTH);
                value = value.substring(0, Math.max(0, truncateWidth - 3)) + "...";
            }
            
            // Pad to width
            value = String.format("%-" + width + "s", value);
            System.out.printf("| %s ", value);
        }
        System.out.println("|");
    }
    
    private String escapeCSV(String value) {
        if (value == null) {
            return "";
        }
        
        // If value contains special characters, quote it
        if (value.contains(CSV_SEPARATOR) || value.contains("\"") || 
            value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        
        return value;
    }
} 
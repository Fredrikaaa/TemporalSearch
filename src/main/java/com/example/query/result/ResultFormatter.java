package com.example.query.result;

import com.example.query.model.ResultTable;
import com.example.query.model.column.ColumnSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Formats result tables for display.
 * Responsible for determining column widths, formatting cell values, and creating formatted text.
 */
public class ResultFormatter {
    private static final Logger logger = LoggerFactory.getLogger(ResultFormatter.class);
    
    /**
     * Formats a result table for display
     *
     * @param resultTable The result table to format
     * @return Formatted string representation of the table
     */
    public String format(ResultTable resultTable) {
        return format(resultTable, new FormattingOptions());
    }
    
    /**
     * Formats a result table with custom options
     *
     * @param resultTable The result table to format
     * @param options Formatting options
     * @return Formatted string representation of the table
     */
    public String format(ResultTable resultTable, FormattingOptions options) {
        logger.debug("Formatting result table with {} columns and {} rows",
                resultTable.getColumns().size(), resultTable.getRows().size());
        
        if (resultTable.getRows().isEmpty()) {
            return "No results found.";
        }
        
        // Get column widths
        List<Integer> columnWidths = calculateColumnWidths(resultTable, options);
        
        // Build the formatted table
        StringBuilder sb = new StringBuilder();
        
        // Add header row if requested
        if (options.isShowHeaders()) {
            appendHeaderRow(sb, resultTable, columnWidths, options);
            appendSeparatorRow(sb, columnWidths);
        }
        
        // Add data rows
        appendDataRows(sb, resultTable, columnWidths, options);
        
        return sb.toString();
    }
    
    /**
     * Calculates the width for each column.
     *
     * @param resultTable The result table
     * @param options Formatting options
     * @return List of column widths
     */
    private List<Integer> calculateColumnWidths(ResultTable resultTable, FormattingOptions options) {
        List<Integer> widths = new ArrayList<>();
        List<ColumnSpec> columns = resultTable.getColumns();
        List<Map<String, String>> rows = resultTable.getRows();
        
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec column = columns.get(i);
            String columnName = column.name();
            int width = columnName != null ? columnName.length() : 0;
            
            // Check data values
            for (Map<String, String> row : rows) {
                String value = row.get(columnName);
                if (value != null) {
                    width = Math.max(width, value.length());
                }
            }
            
            // Apply max width constraint
            width = Math.min(width, options.getMaxColumnWidth());
            widths.add(width);
        }
        
        return widths;
    }
    
    /**
     * Appends the header row to the string builder.
     *
     * @param sb The string builder
     * @param resultTable The result table
     * @param columnWidths Column widths
     * @param options Formatting options
     */
    private void appendHeaderRow(StringBuilder sb, ResultTable resultTable, 
                                List<Integer> columnWidths, FormattingOptions options) {
        List<ColumnSpec> columns = resultTable.getColumns();
        
        // Add row number column if requested
        if (options.isShowRowNumbers()) {
            sb.append(String.format("%-4s", "#"));
            sb.append(" | ");
        }
        
        // Add column headers
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec column = columns.get(i);
            String columnName = column.name();
            sb.append(String.format("%-" + columnWidths.get(i) + "s", columnName));
            
            if (i < columns.size() - 1) {
                sb.append(" | ");
            }
        }
        
        sb.append("\n");
    }
    
    /**
     * Appends a separator row to the string builder.
     *
     * @param sb The string builder
     * @param columnWidths Column widths
     */
    private void appendSeparatorRow(StringBuilder sb, List<Integer> columnWidths) {
        for (int i = 0; i < columnWidths.size(); i++) {
            for (int j = 0; j < columnWidths.get(i); j++) {
                sb.append("-");
            }
            
            if (i < columnWidths.size() - 1) {
                sb.append("-+-");
            }
        }
        
        sb.append("\n");
    }
    
    /**
     * Appends data rows to the string builder.
     *
     * @param sb The string builder
     * @param resultTable The result table
     * @param columnWidths Column widths
     * @param options Formatting options
     */
    private void appendDataRows(StringBuilder sb, ResultTable resultTable, 
                               List<Integer> columnWidths, FormattingOptions options) {
        List<ColumnSpec> columns = resultTable.getColumns();
        List<Map<String, String>> rows = resultTable.getRows();
        
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            Map<String, String> row = rows.get(rowIndex);
            
            // Add row number if requested
            if (options.isShowRowNumbers()) {
                sb.append(String.format("%-4d", rowIndex + 1));
                sb.append(" | ");
            }
            
            // Add cell values
            for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
                ColumnSpec column = columns.get(colIndex);
                String columnName = column.name();
                String value = row.get(columnName);
                
                if (value == null) {
                    value = "";
                } else if (options.isTruncateLongValues() && value.length() > columnWidths.get(colIndex)) {
                    value = value.substring(0, columnWidths.get(colIndex) - 3) + "...";
                }
                
                sb.append(String.format("%-" + columnWidths.get(colIndex) + "s", value));
                
                if (colIndex < columns.size() - 1) {
                    sb.append(" | ");
                }
            }
            
            sb.append("\n");
        }
    }
    
    /**
     * Class for specifying formatting options
     */
    public static class FormattingOptions {
        private final boolean showHeaders;
        private final boolean showRowNumbers;
        private final int maxColumnWidth;
        private final boolean truncateLongValues;
        
        /**
         * Creates a new FormattingOptions with default values
         */
        public FormattingOptions() {
            this.showHeaders = true;
            this.showRowNumbers = false;
            this.maxColumnWidth = 50;
            this.truncateLongValues = true;
        }
        
        /**
         * Creates a new FormattingOptions with custom values
         *
         * @param showHeaders Whether to show column headers
         * @param showRowNumbers Whether to show row numbers
         * @param maxColumnWidth Maximum width for columns
         * @param truncateLongValues Whether to truncate long values
         */
        public FormattingOptions(boolean showHeaders, boolean showRowNumbers, 
                                int maxColumnWidth, boolean truncateLongValues) {
            this.showHeaders = showHeaders;
            this.showRowNumbers = showRowNumbers;
            this.maxColumnWidth = maxColumnWidth;
            this.truncateLongValues = truncateLongValues;
        }
        
        public boolean isShowHeaders() {
            return showHeaders;
        }
        
        public boolean isShowRowNumbers() {
            return showRowNumbers;
        }
        
        public int getMaxColumnWidth() {
            return maxColumnWidth;
        }
        
        public boolean isTruncateLongValues() {
            return truncateLongValues;
        }
    }
} 
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
            appendSeparatorRow(sb, columnWidths, options);
        }
        
        // Add data rows
        appendDataRows(sb, resultTable, columnWidths, options);
        
        return sb.toString();
    }
    
    /**
     * Calculates the width of each column based on its content.
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
            
            // Use a consistent max width regardless of column type
            int effectiveMaxWidth = options.getMaxColumnWidth();
            
            // Check data values
            for (Map<String, String> row : rows) {
                String value = row.get(columnName);
                if (value != null) {
                    // For both single-line and multi-line mode, find the content width
                    width = Math.max(width, Math.min(value.length(), effectiveMaxWidth));
                }
            }
            
            // Apply max width constraint to all columns
            width = Math.min(width, effectiveMaxWidth);
            
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
     * @param options Formatting options
     */
    private void appendSeparatorRow(StringBuilder sb, List<Integer> columnWidths, FormattingOptions options) {
        // Add separator for row number column if needed
        if (options.isShowRowNumbers()) {
            for (int j = 0; j < 4; j++) {
                sb.append("-");
            }
            sb.append("-+-");
        }
        
        // Add separators for data columns
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
            
            if (options.isUseMultiLine()) {
                appendMultiLineRow(sb, row, columns, columnWidths, rowIndex, options);
            } else {
                appendSingleLineRow(sb, row, columns, columnWidths, rowIndex, options);
            }
            
            // Add a separator between rows for multi-line mode to improve readability
            if (options.isUseMultiLine() && rowIndex < rows.size() - 1) {
                appendSeparatorRow(sb, columnWidths, options);
            }
        }
    }
    
    /**
     * Appends a single line row to the string builder.
     *
     * @param sb The string builder
     * @param row The row data
     * @param columns The column specifications
     * @param columnWidths The column widths
     * @param rowIndex The index of the row
     * @param options Formatting options
     */
    private void appendSingleLineRow(StringBuilder sb, Map<String, String> row, 
                                    List<ColumnSpec> columns, List<Integer> columnWidths,
                                    int rowIndex, FormattingOptions options) {
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
                // Handle truncation for single-line mode
                boolean isSnippet = columnName != null && columnName.startsWith("snippet_");
                if (isSnippet) {
                    value = value.substring(0, columnWidths.get(colIndex) - 3) + "...";
                } else {
                    value = value.substring(0, columnWidths.get(colIndex) - 3) + "...";
                }
            }
            
            sb.append(String.format("%-" + columnWidths.get(colIndex) + "s", value));
            
            if (colIndex < columns.size() - 1) {
                sb.append(" | ");
            }
        }
        
        sb.append("\n");
    }
    
    /**
     * Appends a multi-line row to the string builder.
     *
     * @param sb The string builder
     * @param row The row data
     * @param columns The column specifications
     * @param columnWidths The column widths
     * @param rowIndex The index of the row
     * @param options Formatting options
     */
    private void appendMultiLineRow(StringBuilder sb, Map<String, String> row, 
                                    List<ColumnSpec> columns, List<Integer> columnWidths,
                                    int rowIndex, FormattingOptions options) {
        // Get wrapped text for each column
        List<List<String>> wrappedColumnValues = new ArrayList<>();
        int maxLines = 1;
        
        for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
            ColumnSpec column = columns.get(colIndex);
            String columnName = column.name();
            String value = row.get(columnName);
            
            if (value == null) {
                value = "";
            }
            
            boolean isSnippet = columnName != null && columnName.startsWith("snippet_");
            int width = columnWidths.get(colIndex);
            
            List<String> wrappedLines = wrapText(value, width, isSnippet);
            wrappedColumnValues.add(wrappedLines);
            maxLines = Math.max(maxLines, wrappedLines.size());
        }
        
        // Render each line
        for (int lineNum = 0; lineNum < maxLines; lineNum++) {
            // Add row number only on first line if requested
            if (options.isShowRowNumbers()) {
                if (lineNum == 0) {
                    sb.append(String.format("%-4d", rowIndex + 1));
                } else {
                    sb.append(String.format("%-4s", ""));
                }
                sb.append(" | ");
            }
            
            // Add cell values for this line
            for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
                List<String> wrappedLines = wrappedColumnValues.get(colIndex);
                String lineValue = lineNum < wrappedLines.size() ? wrappedLines.get(lineNum) : "";
                
                sb.append(String.format("%-" + columnWidths.get(colIndex) + "s", lineValue));
                
                if (colIndex < columns.size() - 1) {
                    sb.append(" | ");
                }
            }
            
            sb.append("\n");
        }
    }
    
    /**
     * Wraps text to fit within a specified width.
     *
     * @param text The text to wrap
     * @param width The maximum width per line
     * @param isSnippet Whether this is a snippet column (no longer affects wrapping style)
     * @return List of wrapped lines
     */
    private List<String> wrapText(String text, int width, boolean isSnippet) {
        List<String> lines = new ArrayList<>();
        
        if (text.isEmpty()) {
            lines.add("");
            return lines;
        }
        
        // For very narrow columns, ensure we have at least minimum width
        width = Math.max(width, 10);
        
        StringBuilder currentLine = new StringBuilder();
        int currentLineWidth = 0;
        String[] words = text.split("\\s+");
        
        for (String word : words) {
            if (currentLineWidth + word.length() > width) {
                // This word would exceed the width, so start a new line
                lines.add(currentLine.toString());
                currentLine = new StringBuilder();
                currentLineWidth = 0;
            }
            
            if (currentLineWidth > 0) {
                // Not the first word on the line, add a space
                currentLine.append(" ");
                currentLineWidth++;
            }
            
            // For very long words that exceed width, we need to break them
            if (word.length() > width) {
                if (currentLineWidth > 0) {
                    // Finish current line if we've already started it
                    lines.add(currentLine.toString());
                    currentLine = new StringBuilder();
                    currentLineWidth = 0;
                }
                
                // Break the word into chunks
                for (int i = 0; i < word.length(); i += width) {
                    int end = Math.min(i + width, word.length());
                    if (i > 0) {
                        lines.add(word.substring(i, end));
                    } else {
                        // First chunk goes on the current line
                        currentLine.append(word.substring(i, end));
                        currentLineWidth = end - i;
                    }
                }
            } else {
                // Normal case - just add the word
                currentLine.append(word);
                currentLineWidth += word.length();
            }
        }
        
        // Add the final line if not empty
        if (currentLineWidth > 0) {
            lines.add(currentLine.toString());
        }
        
        return lines;
    }
    
    /**
     * Class for specifying formatting options
     */
    public static class FormattingOptions {
        private final boolean showHeaders;
        private final boolean showRowNumbers;
        private final int maxColumnWidth;
        private final int snippetColumnWidth;
        private final boolean truncateLongValues;
        private final boolean useMultiLine;
        
        /**
         * Creates a new FormattingOptions with default values
         */
        public FormattingOptions() {
            this.showHeaders = true;
            this.showRowNumbers = false;
            this.maxColumnWidth = 75;
            this.snippetColumnWidth = 75; // Keep for backward compatibility, but not used
            this.truncateLongValues = true;
            this.useMultiLine = true; // Enable multi-line output by default
        }
        
        /**
         * Creates a new FormattingOptions with custom values including snippet column width
         *
         * @param showHeaders Whether to show column headers
         * @param showRowNumbers Whether to show row numbers
         * @param maxColumnWidth Maximum width for all columns
         * @param snippetColumnWidth Maximum width specifically for snippet columns (no longer used)
         * @param truncateLongValues Whether to truncate long values
         * @param useMultiLine Whether to use multi-line output for long values
         */
        public FormattingOptions(boolean showHeaders, boolean showRowNumbers, 
                                int maxColumnWidth, int snippetColumnWidth, 
                                boolean truncateLongValues, boolean useMultiLine) {
            this.showHeaders = showHeaders;
            this.showRowNumbers = showRowNumbers;
            this.maxColumnWidth = maxColumnWidth;
            this.snippetColumnWidth = snippetColumnWidth; // Keep for backward compatibility, but not used
            this.truncateLongValues = truncateLongValues;
            this.useMultiLine = useMultiLine;
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
        
        public int getSnippetColumnWidth() {
            return snippetColumnWidth;
        }
        
        public boolean isTruncateLongValues() {
            return truncateLongValues;
        }
        
        public boolean isUseMultiLine() {
            return useMultiLine;
        }
    }
} 
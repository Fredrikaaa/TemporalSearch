package com.example.query.model.table;

import com.example.query.model.column.ColumnSpec;
import java.util.List;
import java.util.Map;

/**
 * Represents a table of query results.
 */
public record ResultTable(
    List<ColumnSpec> columns,           // Column specifications
    List<Map<String, String>> rows,     // Row data as maps of column name to value
    int previewLimit,                   // Maximum number of rows to preview
    TableConfig config                  // Table formatting configuration
) {
    public ResultTable {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be null or empty");
        }
        if (rows == null) {
            throw new IllegalArgumentException("rows must not be null");
        }
        if (previewLimit < 1) {
            throw new IllegalArgumentException("previewLimit must be at least 1");
        }
        if (config == null) {
            config = TableConfig.DEFAULT;
        }
    }
    
    /**
     * Creates a new ResultTable with default configuration.
     */
    public static ResultTable create(List<ColumnSpec> columns, List<Map<String, String>> rows) {
        return new ResultTable(columns, rows, 10, TableConfig.DEFAULT);
    }
    
    /**
     * Creates a new ResultTable with custom preview limit.
     */
    public static ResultTable create(List<ColumnSpec> columns, List<Map<String, String>> rows, 
            int previewLimit) {
        return new ResultTable(columns, rows, previewLimit, TableConfig.DEFAULT);
    }
} 
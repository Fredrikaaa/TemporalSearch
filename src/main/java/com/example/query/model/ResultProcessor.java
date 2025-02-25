package com.example.query.model;

import com.example.query.format.TableConfig;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Processor for applying result control operations like sorting, limiting, and aggregation.
 */
public class ResultProcessor {

    /**
     * Applies ordering to a result table based on the provided order specifications.
     *
     * @param table The result table to order
     * @param orderSpecs The list of order specifications to apply
     * @return A new result table with the rows ordered according to the specifications
     */
    public ResultTable applyOrdering(ResultTable table, List<OrderSpec> orderSpecs) {
        if (orderSpecs == null || orderSpecs.isEmpty()) {
            return table;
        }

        // Create a copy of the rows to sort
        List<Map<String, String>> sortedRows = new ArrayList<>(table.rows());
        
        // Sort the rows using a comparator chain based on the order specifications
        sortedRows.sort((row1, row2) -> {
            for (OrderSpec spec : orderSpecs) {
                String field = spec.getField();
                
                // Validate that the field exists in the table
                if (!row1.containsKey(field) || !row2.containsKey(field)) {
                    throw new IllegalArgumentException("Order by field not found in result table: " + field);
                }
                
                String value1 = row1.get(field);
                String value2 = row2.get(field);
                
                // Handle null values (null comes before non-null)
                if (value1 == null && value2 == null) {
                    continue;
                } else if (value1 == null) {
                    return spec.getDirection() == OrderSpec.Direction.ASC ? -1 : 1;
                } else if (value2 == null) {
                    return spec.getDirection() == OrderSpec.Direction.ASC ? 1 : -1;
                }
                
                int comparison = value1.compareTo(value2);
                if (comparison != 0) {
                    return spec.getDirection() == OrderSpec.Direction.ASC ? comparison : -comparison;
                }
            }
            return 0; // All fields are equal
        });
        
        // Create a new result table with the sorted rows
        return new ResultTable(table.columns(), sortedRows, table.previewLimit(), table.config());
    }
    
    /**
     * Applies a limit to the number of rows in a result table.
     *
     * @param table The result table to limit
     * @param limit The maximum number of rows to include
     * @return A new result table with at most the specified number of rows
     */
    public ResultTable applyLimit(ResultTable table, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        
        if (limit >= table.getRowCount()) {
            return table;
        }
        
        // Create a sublist of the rows up to the limit
        List<Map<String, String>> limitedRows = table.rows().subList(0, limit);
        
        // Create a new result table with the limited rows
        return new ResultTable(table.columns(), limitedRows, table.previewLimit(), table.config());
    }
    
    /**
     * Counts the total number of rows in a result table.
     *
     * @param table The result table to count
     * @return The total number of rows
     */
    public int countAll(ResultTable table) {
        return table.getRowCount();
    }
    
    /**
     * Counts the number of unique values in a specific column.
     *
     * @param table The result table to process
     * @param columnName The name of the column to count unique values for
     * @return The number of unique values in the specified column
     */
    public int countUnique(ResultTable table, String columnName) {
        // Validate that the column exists
        if (!table.getColumnNames().contains(columnName)) {
            throw new IllegalArgumentException("Column not found in result table: " + columnName);
        }
        
        // Extract the column values and count unique ones
        Set<String> uniqueValues = new HashSet<>();
        for (int i = 0; i < table.getRowCount(); i++) {
            String value = table.getValue(i, columnName);
            if (value != null) {
                uniqueValues.add(value);
            }
        }
        
        return uniqueValues.size();
    }
    
    /**
     * Counts the number of unique documents in a result table.
     * This assumes there is a "document_id" column in the table.
     *
     * @param table The result table to process
     * @return The number of unique documents
     */
    public int countDocuments(ResultTable table) {
        // Check if the table has a document_id column
        if (!table.getColumnNames().contains("document_id")) {
            throw new IllegalArgumentException("Result table does not contain document_id column");
        }
        
        return countUnique(table, "document_id");
    }
    
    /**
     * Creates a result table containing a single count value.
     *
     * @param count The count value
     * @param countType The type of count (ALL, UNIQUE, DOCUMENTS)
     * @param columnName The name of the column counted (for UNIQUE counts)
     * @return A result table with a single row containing the count value
     */
    public ResultTable createCountResultTable(int count, CountNode.CountType countType, String columnName) {
        // Create a column for the count
        String countColumnName = getCountColumnName(countType, columnName);
        List<ColumnSpec> columns = List.of(
            new ColumnSpec(countColumnName, ColumnType.COUNT)
        );
        
        // Create a single row with the count value
        Map<String, String> row = new HashMap<>();
        row.put(countColumnName, String.valueOf(count));
        List<Map<String, String>> rows = List.of(row);
        
        // Create and return the result table
        return new ResultTable(columns, rows, 1, TableConfig.getDefault());
    }
    
    private String getCountColumnName(CountNode.CountType countType, String columnName) {
        return switch (countType) {
            case ALL -> "COUNT(*)";
            case UNIQUE -> "COUNT(UNIQUE " + columnName + ")";
            case DOCUMENTS -> "COUNT(DOCUMENTS)";
        };
    }
} 
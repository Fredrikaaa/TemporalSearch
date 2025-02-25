package com.example.query.model;

import com.example.query.format.TableConfig;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A simplified result table that combines functionality from ResultTable, ResultTableBuilder,
 * and essential parts of ResultProcessor for easier maintenance.
 */
public class ResultTable {
    private final List<ColumnSpec> columns;
    private final List<Map<String, String>> rows;
    private final int previewLimit;
    private final TableConfig config;

    /**
     * Creates a new ResultTable with default configuration.
     */
    public ResultTable() {
        this.columns = new ArrayList<>();
        this.rows = new ArrayList<>();
        this.previewLimit = 10;
        this.config = TableConfig.getDefault();
    }

    /**
     * Creates a new ResultTable with specific columns.
     *
     * @param columns The column specifications
     */
    public ResultTable(List<ColumnSpec> columns) {
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>();
        this.previewLimit = 10;
        this.config = TableConfig.getDefault();
    }

    /**
     * Creates a new ResultTable with specific columns and rows.
     *
     * @param columns The column specifications
     * @param rows The data rows
     */
    public ResultTable(List<ColumnSpec> columns, List<Map<String, String>> rows) {
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>(rows);
        this.previewLimit = 10;
        this.config = TableConfig.getDefault();
    }

    /**
     * Creates a new ResultTable with all parameters.
     *
     * @param columns The column specifications
     * @param rows The data rows
     * @param previewLimit The maximum number of rows to display in preview
     * @param config The table formatting configuration
     */
    public ResultTable(List<ColumnSpec> columns, List<Map<String, String>> rows, 
                           int previewLimit, TableConfig config) {
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>(rows);
        this.previewLimit = previewLimit;
        this.config = config;
    }

    /**
     * Gets the columns in this table.
     *
     * @return The list of column specifications
     */
    public List<ColumnSpec> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     * Gets the rows in this table.
     *
     * @return The list of data rows
     */
    public List<Map<String, String>> getRows() {
        return Collections.unmodifiableList(rows);
    }

    /**
     * Gets the preview limit for this table.
     *
     * @return The preview limit
     */
    public int getPreviewLimit() {
        return previewLimit;
    }

    /**
     * Gets the table configuration.
     *
     * @return The table configuration
     */
    public TableConfig getConfig() {
        return config;
    }

    /**
     * Gets the number of rows in the table.
     *
     * @return The row count
     */
    public int getRowCount() {
        return rows.size();
    }

    /**
     * Gets the number of columns in the table.
     *
     * @return The column count
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Gets the column names in order.
     *
     * @return The list of column names
     */
    public List<String> getColumnNames() {
        return columns.stream()
            .map(ColumnSpec::name)
            .collect(Collectors.toList());
    }

    /**
     * Gets the value at the specified row and column.
     *
     * @param rowIndex The row index
     * @param columnName The column name
     * @return The value at the specified position
     * @throws IllegalArgumentException if the column name is invalid
     * @throws IndexOutOfBoundsException if the row index is out of bounds
     */
    public String getValue(int rowIndex, String columnName) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
        }

        Map<String, String> row = rows.get(rowIndex);
        if (!row.containsKey(columnName)) {
            throw new IllegalArgumentException("Invalid column name: " + columnName);
        }

        return row.get(columnName);
    }

    // Builder methods for fluent API

    /**
     * Adds a column to the table.
     *
     * @param column The column specification to add
     * @return This table for method chaining
     */
    public ResultTable addColumn(ColumnSpec column) {
        this.columns.add(column);
        return this;
    }

    /**
     * Adds multiple columns to the table.
     *
     * @param columnsToAdd The column specifications to add
     * @return This table for method chaining
     */
    public ResultTable addColumns(List<ColumnSpec> columnsToAdd) {
        this.columns.addAll(columnsToAdd);
        return this;
    }

    /**
     * Adds a row to the table.
     *
     * @param values The map of column names to values
     * @return This table for method chaining
     */
    public ResultTable addRow(Map<String, String> values) {
        this.rows.add(new HashMap<>(values));
        return this;
    }

    /**
     * Adds multiple rows to the table.
     *
     * @param rowsToAdd The list of rows to add
     * @return This table for method chaining
     */
    public ResultTable addRows(List<Map<String, String>> rowsToAdd) {
        rowsToAdd.forEach(row -> this.rows.add(new HashMap<>(row)));
        return this;
    }

    // Processing methods from ResultProcessor

    /**
     * Sorts the table rows based on the given order specifications.
     *
     * @param orderSpecs The order specifications
     * @return A new table with sorted rows
     */
    public ResultTable sort(List<OrderSpec> orderSpecs) {
        if (orderSpecs == null || orderSpecs.isEmpty()) {
            return this;
        }

        List<Map<String, String>> sortedRows = new ArrayList<>(this.rows);
        
        sortedRows.sort((row1, row2) -> {
            for (OrderSpec spec : orderSpecs) {
                String field = spec.getField();
                
                if (!row1.containsKey(field) || !row2.containsKey(field)) {
                    throw new IllegalArgumentException("Order by field not found: " + field);
                }
                
                String value1 = row1.get(field);
                String value2 = row2.get(field);
                
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
            return 0;
        });
        
        return new ResultTable(this.columns, sortedRows, this.previewLimit, this.config);
    }

    /**
     * Limits the number of rows in the table.
     *
     * @param limit The maximum number of rows
     * @return A new table with at most the specified number of rows
     */
    public ResultTable limit(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }
        
        if (limit >= this.rows.size()) {
            return this;
        }
        
        List<Map<String, String>> limitedRows = this.rows.subList(0, limit);
        
        return new ResultTable(this.columns, limitedRows, this.previewLimit, this.config);
    }

    /**
     * Counts the total number of rows in the table.
     *
     * @return The total row count
     */
    public int countAll() {
        return this.rows.size();
    }

    /**
     * Counts the number of unique values in a specific column.
     *
     * @param columnName The name of the column to count unique values for
     * @return The number of unique values in the specified column
     */
    public int countUnique(String columnName) {
        if (!getColumnNames().contains(columnName)) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        
        Set<String> uniqueValues = new HashSet<>();
        for (int i = 0; i < getRowCount(); i++) {
            String value = getValue(i, columnName);
            if (value != null) {
                uniqueValues.add(value);
            }
        }
        
        return uniqueValues.size();
    }

    /**
     * Creates a new table containing a single count value.
     *
     * @param count The count value
     * @param columnName The name of the count column
     * @return A new table with a single row containing the count value
     */
    public static ResultTable createCountTable(int count, String columnName) {
        List<ColumnSpec> columns = List.of(
            new ColumnSpec(columnName, ColumnType.COUNT)
        );
        
        Map<String, String> row = new HashMap<>();
        row.put(columnName, String.valueOf(count));
        List<Map<String, String>> rows = List.of(row);
        
        return new ResultTable(columns, rows, 1, TableConfig.getDefault());
    }

    /**
     * Formats the table as a string representation for display.
     *
     * @return A string representation of the table
     */
    public String format() {
        if (columns.isEmpty() || rows.isEmpty()) {
            return "[Empty result table]";
        }

        // Get column names and determine column widths
        List<String> columnNames = getColumnNames();
        Map<String, Integer> columnWidths = new HashMap<>();
        
        // Initialize with column name lengths
        for (String name : columnNames) {
            columnWidths.put(name, name.length());
        }
        
        // Adjust for data widths (up to max width)
        int maxWidth = config.maxColumnWidth();
        for (Map<String, String> row : rows) {
            for (String col : columnNames) {
                String value = row.getOrDefault(col, config.nullValueDisplay());
                int displayLength = Math.min(value.length(), maxWidth);
                columnWidths.put(col, Math.max(columnWidths.get(col), displayLength));
            }
        }
        
        StringBuilder sb = new StringBuilder();
        
        // Format header
        if (config.showHeaders()) {
            for (String col : columnNames) {
                sb.append(String.format("| %-" + columnWidths.get(col) + "s ", col));
            }
            sb.append("|\n");
            
            // Format header separator
            for (String col : columnNames) {
                sb.append("+");
                sb.append("-".repeat(columnWidths.get(col) + 2));
            }
            sb.append("+\n");
        }
        
        // Format data rows (limited by preview limit)
        int rowsToShow = Math.min(rows.size(), previewLimit);
        for (int i = 0; i < rowsToShow; i++) {
            Map<String, String> row = rows.get(i);
            for (String col : columnNames) {
                String value = row.getOrDefault(col, config.nullValueDisplay());
                if (value.length() > maxWidth) {
                    value = value.substring(0, maxWidth - 3) + "...";
                }
                sb.append(String.format("| %-" + columnWidths.get(col) + "s ", value));
            }
            sb.append("|\n");
        }
        
        // Add indicator if there are more rows
        if (rows.size() > previewLimit) {
            sb.append(String.format("[%d more rows not shown]%n", rows.size() - previewLimit));
        }
        
        return sb.toString();
    }

    @Override
    public String toString() {
        return format();
    }
} 
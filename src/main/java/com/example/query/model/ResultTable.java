package com.example.query.model;

import com.example.query.format.TableConfig;
import com.example.query.model.column.ColumnSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a table of query results with column specifications and row data.
 */
public record ResultTable(
    List<ColumnSpec> columns,
    List<Map<String, String>> rows,
    int previewLimit,
    TableConfig config
) {
    /**
     * Creates a new result table with validation.
     */
    public ResultTable {
        Objects.requireNonNull(columns, "Columns cannot be null");
        Objects.requireNonNull(rows, "Rows cannot be null");
        Objects.requireNonNull(config, "Table config cannot be null");

        if (previewLimit < 0) {
            throw new IllegalArgumentException("Preview limit must be non-negative");
        }

        // Defensive copies
        columns = new ArrayList<>(columns);
        rows = new ArrayList<>(rows);
    }

    /**
     * Gets the number of rows in the result table.
     * @return The row count
     */
    public int getRowCount() {
        return rows.size();
    }

    /**
     * Gets the number of columns in the result table.
     * @return The column count
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Gets the column names in order.
     * @return The list of column names
     */
    public List<String> getColumnNames() {
        return columns.stream()
            .map(ColumnSpec::name)
            .toList();
    }

    /**
     * Gets the value at the specified row and column.
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

    /**
     * Creates a builder for constructing result tables.
     * @return A new ResultTableBuilder
     */
    public static ResultTableBuilder builder() {
        return new ResultTableBuilder();
    }
} 
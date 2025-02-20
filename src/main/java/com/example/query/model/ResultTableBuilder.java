package com.example.query.model;

import com.example.query.format.TableConfig;
import com.example.query.model.column.ColumnSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for constructing ResultTable instances.
 */
public class ResultTableBuilder {
    private final List<ColumnSpec> columns = new ArrayList<>();
    private final List<Map<String, String>> rows = new ArrayList<>();
    private int previewLimit = 10;
    private TableConfig config = TableConfig.getDefault();

    /**
     * Adds a column specification to the table.
     * @param column The column specification to add
     * @return This builder for chaining
     */
    public ResultTableBuilder addColumn(ColumnSpec column) {
        columns.add(column);
        return this;
    }

    /**
     * Adds multiple column specifications to the table.
     * @param columns The column specifications to add
     * @return This builder for chaining
     */
    public ResultTableBuilder addColumns(List<ColumnSpec> columns) {
        this.columns.addAll(columns);
        return this;
    }

    /**
     * Adds a row of values to the table.
     * @param values The map of column names to values
     * @return This builder for chaining
     */
    public ResultTableBuilder addRow(Map<String, String> values) {
        rows.add(new HashMap<>(values));
        return this;
    }

    /**
     * Adds multiple rows to the table.
     * @param rows The list of rows to add
     * @return This builder for chaining
     */
    public ResultTableBuilder addRows(List<Map<String, String>> rows) {
        rows.forEach(this::addRow);
        return this;
    }

    /**
     * Sets the preview limit for the table.
     * @param limit The maximum number of rows to show in preview
     * @return This builder for chaining
     */
    public ResultTableBuilder previewLimit(int limit) {
        this.previewLimit = limit;
        return this;
    }

    /**
     * Sets the table configuration.
     * @param config The table configuration
     * @return This builder for chaining
     */
    public ResultTableBuilder config(TableConfig config) {
        this.config = config;
        return this;
    }

    /**
     * Builds a new ResultTable instance.
     * @return The constructed ResultTable
     */
    public ResultTable build() {
        return new ResultTable(columns, rows, previewLimit, config);
    }
} 
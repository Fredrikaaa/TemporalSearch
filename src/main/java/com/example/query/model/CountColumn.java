package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import tech.tablesaw.selection.Selection;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents a COUNT expression in the SELECT clause of a query.
 * This column performs counting operations using Tablesaw's built-in aggregation features.
 */
public class CountColumn implements SelectColumn {
    public enum CountType {
        ALL,       // COUNT(*)
        UNIQUE,    // COUNT(UNIQUE ?var)
        DOCUMENTS  // COUNT(DOCUMENTS)
    }
    
    private final CountType type;
    private final String variable;
    private final String columnName;
    
    /**
     * Creates a COUNT(*) column.
     */
    public static CountColumn countAll() {
        return new CountColumn(CountType.ALL, null, "count");
    }
    
    /**
     * Creates a COUNT(UNIQUE ?var) column.
     *
     * @param variable The variable to count unique values of
     */
    public static CountColumn countUnique(String variable) {
        return new CountColumn(CountType.UNIQUE, variable, "count_" + variable);
    }
    
    /**
     * Creates a COUNT(DOCUMENTS) column.
     */
    public static CountColumn countDocuments() {
        return new CountColumn(CountType.DOCUMENTS, null, "document_count");
    }
    
    /**
     * Creates a new count column.
     * 
     * @param type The type of count operation
     * @param variable The variable to count (for UNIQUE counts)
     * @param columnName The name for the column in the result table
     */
    private CountColumn(CountType type, String variable, String columnName) {
        this.type = type;
        this.variable = variable;
        this.columnName = columnName;
    }
    
    @Override
    public String getColumnName() {
        return columnName;
    }
    
    @Override
    public Column<?> createColumn() {
        return IntColumn.create(getColumnName());
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, DocSentenceMatch match, 
                              BindingContext bindingContext, Map<String, IndexAccess> indexes) {
        IntColumn column = (IntColumn) table.column(getColumnName());
        
        switch (type) {
            case ALL -> column.set(rowIndex, 1); // Will be summed later if grouping is needed
            case DOCUMENTS -> {
                // For document counts, we'll set 1 if this is the first occurrence of the document
                String docId = String.valueOf(match.documentId());
                boolean isFirstOccurrence = table.column("document_id").asStringColumn()
                    .indexOf(docId) == rowIndex;
                column.set(rowIndex, isFirstOccurrence ? 1 : 0);
            }
            case UNIQUE -> {
                if (variable != null && bindingContext.hasValue(variable)) {
                    // For unique counting, we'll do a post-processing step after all rows are populated
                    // For now, just set a placeholder
                    column.set(rowIndex, 1);
                } else {
                    column.set(rowIndex, 0);
                }
            }
        }
    }
    
    /**
     * Performs post-processing on the table to compute accurate counts.
     * This is called after all rows have been populated.
     *
     * @param table The result table
     * @return The table with count columns properly aggregated
     */
    public static Table applyCountAggregations(Table table) {
        // For simple counts, sum the count column
        if (table.containsColumn("count")) {
            int total = (int) table.intColumn("count").sum();
            
            // If we have only one row, update the count directly
            if (table.rowCount() == 1) {
                table.intColumn("count").set(0, total);
            } 
            // Otherwise create a summarized table with just the count
            else if (table.rowCount() > 1) {
                Table countTable = Table.create("CountResult");
                countTable.addColumns(IntColumn.create("count", new int[]{total}));
                return countTable;
            }
        }
        
        // For document counts, count distinct document IDs
        if (table.containsColumn("document_count") && table.containsColumn("document_id")) {
            // Get unique document IDs using a Set
            StringColumn docIdColumn = table.stringColumn("document_id");
            Set<String> uniqueDocIds = new HashSet<>();
            for (String docId : docIdColumn) {
                uniqueDocIds.add(docId);
            }
            int docCount = uniqueDocIds.size();
            
            // If we have only one row, update the count directly
            if (table.rowCount() == 1) {
                table.intColumn("document_count").set(0, docCount);
            } 
            // Otherwise create a summarized table with just the count
            else if (table.rowCount() > 1) {
                Table countTable = Table.create("CountResult");
                countTable.addColumns(IntColumn.create("document_count", new int[]{docCount}));
                return countTable;
            }
        }
        
        // For unique variable counts
        for (String colName : table.columnNames()) {
            if (colName.startsWith("count_")) {
                String varColumn = colName.substring(6); // Remove "count_" prefix
                if (table.containsColumn(varColumn)) {
                    // Count unique values manually to avoid type issues
                    Set<Object> uniqueValues = new HashSet<>();
                    Column<?> column = table.column(varColumn);
                    for (int i = 0; i < column.size(); i++) {
                        uniqueValues.add(column.get(i));
                    }
                    int uniqueCount = uniqueValues.size();
                    
                    // If we have only one row, update the count directly
                    if (table.rowCount() == 1) {
                        table.intColumn(colName).set(0, uniqueCount);
                    } 
                    // Otherwise create a summarized table with just the count
                    else if (table.rowCount() > 1) {
                        Table countTable = Table.create("CountResult");
                        countTable.addColumns(IntColumn.create(colName, new int[]{uniqueCount}));
                        return countTable;
                    }
                }
            }
        }
        
        return table;
    }
    
    @Override
    public String toString() {
        return switch (type) {
            case ALL -> "COUNT(*)";
            case UNIQUE -> "COUNT(UNIQUE " + variable + ")";
            case DOCUMENTS -> "COUNT(DOCUMENTS)";
        };
    }
} 
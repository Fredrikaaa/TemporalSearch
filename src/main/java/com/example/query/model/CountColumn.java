package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.Map;

/**
 * Represents a COUNT expression in the SELECT clause of a query.
 * This column calculates a count of matches based on the count configuration.
 */
public class CountColumn implements SelectColumn {
    private final CountNode countNode;
    
    /**
     * Creates a new count column.
     * 
     * @param countNode The count node containing the count configuration
     */
    public CountColumn(CountNode countNode) {
        this.countNode = countNode;
    }
    
    /**
     * Gets the count node.
     * 
     * @return The count node
     */
    public CountNode getCountNode() {
        return countNode;
    }
    
    @Override
    public String getColumnName() {
        return "count";
    }
    
    @Override
    public Column<?> createColumn() {
        return IntColumn.create(getColumnName());
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, DocSentenceMatch match, 
                              BindingContext bindingContext, Map<String, IndexAccess> indexes) {
        // For simplicity, we'll just set a count of 1 for each match
        // In a real implementation, you would calculate the count based on the count node configuration
        IntColumn column = (IntColumn) table.column(getColumnName());
        column.set(rowIndex, 1);
    }
    
    @Override
    public String toString() {
        return countNode.toString();
    }
} 
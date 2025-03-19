package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.Map;

/**
 * Interface for all column types that can appear in a SELECT clause.
 * This includes variables, snippets, and other column expressions.
 */
public interface SelectColumn {
    /**
     * Gets the name of this column as it should appear in results.
     * @return The column name
     */
    String getColumnName();
    
    /**
     * Creates a Tablesaw column for this select column.
     * @return A new Tablesaw column
     */
    Column<?> createColumn();
    
    /**
     * Populates the column with data for the given match.
     * 
     * @param table The table containing the column
     * @param rowIndex The row index to populate
     * @param match The document/sentence match
     * @param bindingContext The binding context
     * @param indexes The indexes for additional document information
     */
    void populateColumn(Table table, int rowIndex, DocSentenceMatch match, 
                        BindingContext bindingContext, Map<String, IndexAccess> indexes);
} 
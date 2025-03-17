package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;

/**
 * Represents a variable column in the SELECT clause of a query.
 * This column selects the value of a variable binding from matching documents.
 */
public class VariableColumn implements SelectColumn {
    private final String variableName;
    
    /**
     * Creates a new variable column.
     * 
     * @param variableName The name of the variable (without ? prefix)
     */
    public VariableColumn(String variableName) {
        this.variableName = variableName;
    }
    
    /**
     * Gets the variable name.
     * 
     * @return The variable name
     */
    public String getVariableName() {
        return variableName;
    }
    
    @Override
    public String getColumnName() {
        return variableName;
    }
    
    @Override
    public Column<?> createColumn() {
        return StringColumn.create(getColumnName());
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, DocSentenceMatch match, 
                              VariableBindings variableBindings, Map<String, IndexAccess> indexes) {
        StringColumn column = (StringColumn) table.column(getColumnName());
        
        String value = null;
        if (match.isSentenceLevel()) {
            Map<String, List<String>> sentVars = variableBindings.getValuesForSentence(match.documentId(), match.sentenceId());
            List<String> values = sentVars.get(variableName);
            if (values != null && !values.isEmpty()) {
                value = values.get(0);
            }
        } else {
            Map<String, List<String>> docVars = variableBindings.getValuesForDocument(match.documentId());
            List<String> values = docVars.get(variableName);
            if (values != null && !values.isEmpty()) {
                value = values.get(0);
            }
        }
        
        column.set(rowIndex, value);
    }
    
    @Override
    public String toString() {
        return "?" + variableName;
    }
} 
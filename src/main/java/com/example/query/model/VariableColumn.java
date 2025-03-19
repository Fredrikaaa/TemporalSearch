package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a variable column in the SELECT clause of a query.
 * This column selects the value of a variable binding from matching documents.
 */
public class VariableColumn implements SelectColumn {
    private static final Logger logger = LoggerFactory.getLogger(VariableColumn.class);
    
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
                              BindingContext bindingContext, Map<String, IndexAccess> indexes) {
        StringColumn column = (StringColumn) table.column(getColumnName());
        
        System.out.println("VariableColumn: Populating column for variable: " + variableName + ", match: " + match);
        
        // Add ? prefix if needed
        String varName = variableName.startsWith("?") ? variableName : "?" + variableName;
        
        // Try to get a String value for this variable
        Optional<String> valueOpt = bindingContext.getValue(varName, String.class);
        String value = null;
        
        if (valueOpt.isPresent()) {
            value = valueOpt.get();
            System.out.println("VariableColumn: Found value: " + value);
        } else {
            // Try to get all values for this variable
            List<String> values = bindingContext.getValues(varName, String.class);
            if (!values.isEmpty()) {
                value = values.get(0);
                System.out.println("VariableColumn: Found value from list: " + value);
            } else {
                System.out.println("VariableColumn: No value found for variable: " + varName);
            }
        }
        
        // Extract the actual entity value from the format "term@beginPos:endPos" if present
        if (value != null) {
            int atIndex = value.indexOf('@');
            if (atIndex > 0) {
                value = value.substring(0, atIndex);
                System.out.println("VariableColumn: Extracted value: " + value);
            }
        } else {
            System.out.println("VariableColumn: No value found for variable: " + variableName);
        }
        
        column.set(rowIndex, value);
    }
    
    @Override
    public String toString() {
        return "?" + variableName;
    }
} 
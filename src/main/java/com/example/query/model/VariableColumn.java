package com.example.query.model;

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
    public String toString() {
        return "?" + variableName;
    }
} 
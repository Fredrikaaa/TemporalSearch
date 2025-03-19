package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

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
                              VariableBindings variableBindings, Map<String, IndexAccess> indexes) {
        StringColumn column = (StringColumn) table.column(getColumnName());
        
        System.out.println("VariableColumn: Populating column for variable: " + variableName + ", match: " + match);
        
        String value = null;
        if (match.isSentenceLevel()) {
            // For sentence-level match, use sentence-level bindings
            Map<String, List<String>> sentVars = variableBindings.getValuesForSentence(match.documentId(), match.sentenceId());
            System.out.println("VariableColumn: Sentence variables: " + sentVars);
            List<String> values = sentVars.get(variableName);
            if (values != null && !values.isEmpty()) {
                value = values.get(0);
                System.out.println("VariableColumn: Found sentence value: " + value);
            }
        } else {
            // For document-level match, first try document-level bindings
            Map<String, List<String>> docVars = variableBindings.getValuesForDocument(match.documentId());
            System.out.println("VariableColumn: Document variables: " + docVars);
            List<String> values = docVars.get(variableName);
            
            if (values != null && !values.isEmpty()) {
                value = values.get(0);
                System.out.println("VariableColumn: Found document value: " + value);
            } else {
                // If no document-level bindings, consolidate all sentence-level bindings for this document
                System.out.println("VariableColumn: No document-level bindings, checking all sentences for document: " + match.documentId());
                
                // Get all sentences with bindings for this document
                Set<String> allValues = new HashSet<>();
                for (VariableBindings.SentenceKey key : variableBindings.getSentenceKeys()) {
                    if (key.getDocumentId() == match.documentId()) {
                        Map<String, List<String>> sentVars = variableBindings.getValuesForSentence(key.getDocumentId(), key.getSentenceId());
                        List<String> sentValues = sentVars.get(variableName);
                        if (sentValues != null && !sentValues.isEmpty()) {
                            allValues.addAll(sentValues);
                            System.out.println("VariableColumn: Found sentence-level binding in sentence " + key.getSentenceId() + ": " + sentValues);
                        }
                    }
                }
                
                if (!allValues.isEmpty()) {
                    // Use the first value we found (or we could join them if multiple are needed)
                    value = new ArrayList<>(allValues).get(0);
                    System.out.println("VariableColumn: Using first sentence-level value: " + value);
                }
            }
        }
        
        // Extract the actual entity value from the format "term@beginPos:endPos"
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
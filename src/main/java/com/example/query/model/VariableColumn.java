package com.example.query.model;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import com.example.query.executor.NerExecutor;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;
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
        
        logger.debug("Populating column for variable: {} for match: {}", variableName, match);
        
        // Add ? prefix if needed
        String varName = variableName.startsWith("?") ? variableName : "?" + variableName;
        
        // Try to get value specifically for this match first
        String value = null;
        try {
            // Check if this match has a specific value for this variable
            Object matchValue = match.getVariableValue(varName);
            if (matchValue != null) {
                if (matchValue instanceof NerExecutor.MatchedEntityValue entityValue) {
                    // Get text from MatchedEntityValue
                    value = entityValue.text();
                    logger.debug("Found match-specific MatchedEntityValue: {}", value);
                } else if (matchValue instanceof String) {
                    value = (String) matchValue;
                    logger.debug("Found match-specific String value: {}", value);
                } else {
                    value = matchValue.toString();
                    logger.debug("Found match-specific value (converted to string): {}", value);
                }
            }
        } catch (Exception e) {
            logger.warn("Error extracting match-specific variable value: {}", e.getMessage());
        }
        
        // If no match-specific value, try the global binding context
        if (value == null) {
            try {
                // Try to get a MatchedEntityValue from binding context
                Optional<NerExecutor.MatchedEntityValue> entityValueOpt = 
                    bindingContext.getValue(varName, NerExecutor.MatchedEntityValue.class);
                
                if (entityValueOpt.isPresent()) {
                    NerExecutor.MatchedEntityValue entityValue = entityValueOpt.get();
                    value = entityValue.text();
                    logger.debug("Found MatchedEntityValue from binding context: {}", value);
                } else {
                    // Try to get a String value
                    Optional<String> stringOpt = bindingContext.getValue(varName, String.class);
                    if (stringOpt.isPresent()) {
                        value = stringOpt.get();
                        logger.debug("Found String value from binding context: {}", value);
                    } else {
                        // Try to get all values for this variable
                        List<Object> values = bindingContext.getValues(varName, Object.class);
                        if (!values.isEmpty()) {
                            Object obj = values.get(0);
                            if (obj instanceof NerExecutor.MatchedEntityValue entityValue) {
                                value = entityValue.text();
                                logger.debug("Found MatchedEntityValue from binding context list: {}", value);
                            } else if (obj instanceof String) {
                                value = (String) obj;
                                logger.debug("Found String value from binding context list: {}", value);
                            } else {
                                value = obj.toString();
                                logger.debug("Found value (converted to string) from binding context list: {}", value);
                            }
                        } else {
                            logger.debug("No value found for variable: {}", varName);
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Error extracting variable value from binding context: {}", e);
            }
        }
        
        if (value == null) {
            logger.debug("No value found for variable: {}", variableName);
        }
        
        column.set(rowIndex, value);
    }
    
    @Override
    public String toString() {
        return "?" + variableName;
    }
} 
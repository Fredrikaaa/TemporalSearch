package com.example.query.model;

import com.example.core.IndexAccessInterface;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.LocalDate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a variable column in the SELECT clause of a query.
 * This column selects the value of a variable binding from matching documents,
 * handling both simple variables (?var) and qualified variables (alias.?var).
 */
public class VariableColumn implements SelectColumn {
    private static final Logger logger = LoggerFactory.getLogger(VariableColumn.class);
    
    private final String columnName; // The full name as it appears in SELECT (e.g., "?var" or "alias.?var")
    private final String alias; // The alias part (e.g., "alias"), or null if unqualified
    private final String targetVariableName; // The simple variable name (e.g., "?var")
    
    // TODO: Infer ColumnType based on VariableRegistry? Currently defaults to String.
    private final ColumnType columnType = ColumnType.STRING; 

    /**
     * Creates a new variable column, parsing qualified names if necessary.
     * 
     * @param nameInSelect The name as it appears in the SELECT clause (e.g., "?var" or "alias.?var")
     */
    public VariableColumn(String nameInSelect) {
        this.columnName = nameInSelect;
        if (nameInSelect.contains(".")) {
            String[] parts = nameInSelect.split("\\.", 2);
            if (parts.length == 2 && parts[1].startsWith("?")) {
                this.alias = parts[0];
                this.targetVariableName = parts[1];
            } else {
                logger.warn("Invalid qualified variable format '{}' treated as simple variable name.", nameInSelect);
                this.alias = null;
                this.targetVariableName = nameInSelect.startsWith("?") ? nameInSelect : "?" + nameInSelect;
            }
        } else {
            this.alias = null;
            // Ensure targetVariableName always starts with ? for unqualified names
            this.targetVariableName = nameInSelect.startsWith("?") ? nameInSelect : "?" + nameInSelect;
        }
        logger.trace("Created VariableColumn: columnName='{}', alias='{}', targetVariableName='{}'", 
                     this.columnName, this.alias, this.targetVariableName);
    }
    
    /**
     * Gets the variable name.
     * 
     * @return The variable name (without '?')
     */
    @Override
    public String getColumnName() {
        return columnName;
    }
    
    @Override
    public Column<?> createColumn() {
        return StringColumn.create(columnName);
    }
    
    @Override
    public void populateColumn(Table table, int rowIndex, List<MatchDetail> detailsForUnit, 
                               String source,
                               Map<String, IndexAccessInterface> indexes) {
        
        logger.trace("Populating row {} for column '{}' (alias: {}, targetVar: {}). Details count: {}", 
                      rowIndex, columnName, alias, targetVariableName, detailsForUnit.size());

        Optional<Object> valueOpt = Optional.empty();

        // Iterate through details to find the one matching our target variable and alias context
        for (MatchDetail detail : detailsForUnit) {
            if (alias == null) {
                // Unqualified variable (?var) - look in the left/main part of the detail
                if (targetVariableName.equals(detail.variableName())) {
                    valueOpt = Optional.ofNullable(detail.value());
                    logger.trace("Found match for unqualified '{}' in detail: {}", targetVariableName, detail);
                    break; // Found the first match
                }
            } else {
                // Qualified variable (alias.?var) - look in the right part of the detail
                if (detail.isJoinResult() && 
                    detail.getRightVariableName().isPresent() && 
                    targetVariableName.equals(detail.getRightVariableName().get())) 
                {
                    valueOpt = detail.getRightValue(); // Already Optional<Object>
                    logger.trace("Found match for qualified '{}' in right side of join detail: {}", columnName, detail);
                    break; // Found the first match
                }
                 // As a fallback or alternative (depending on join implementation), 
                 // check if the detail itself represents the right side *directly*
                 // This depends on how JoinExecutor creates the final list of MatchDetails.
                 // Let's add a log for this case.
                 else if (targetVariableName.equals(detail.variableName())) {
                     logger.trace("Checking potential direct match for qualified '{}' in detail: {}", columnName, detail);
                     // If this detail directly represents the value for alias.?var, uncommenting the next line might be needed
                     // valueOpt = Optional.ofNullable(detail.value());
                     // break;
                 }
            }
        }
            
        Column<?> column = table.column(columnName);
        if (!(column instanceof StringColumn strCol)) {
            logger.error("VariableColumn '{}' requires a StringColumn, but found {}", columnName, (column != null ? column.type() : "null"));
            return; // Cannot proceed if column type is wrong
        }
        
        if (valueOpt.isPresent()) {
            Object value = valueOpt.get();
            strCol.set(rowIndex, value != null ? value.toString() : ""); // Set value from the found detail
            logger.trace("Set value '{}' for column '{}' at row {}", value, columnName, rowIndex);
        } else {
            // No detail matched this variable for this unit
            strCol.setMissing(rowIndex); // Mark as missing
            logger.trace("No matching detail found for column '{}' at row {}, setting missing.", columnName, rowIndex);
        }
    }
    
    @Override
    public String toString() {
        return "?" + targetVariableName;
    }
} 
package com.example.query.executor.strategies;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import com.example.query.executor.TemporalJoinStrategy;
import com.example.query.model.JoinCondition;
import com.example.query.result.ResultGenerationException;

/**
 * A naive implementation of the CONTAINS temporal join predicate.
 * 
 * For simple date columns, this implementation checks for exact date matches.
 * In a more sophisticated implementation, it would check if a date range
 * from the left table completely contains a date range from the right table.
 */
public class NaiveContainsJoinStrategy implements TemporalJoinStrategy {

    @Override
    public Table execute(Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException {
        
        validateInputs(leftTable, rightTable, joinCondition);
        
        String leftColumn = joinCondition.leftColumn();
        String rightColumn = joinCondition.rightColumn();
        JoinCondition.JoinType joinType = joinCondition.type();
        
        // Create a new table with columns from both tables
        Table joinedTable = Table.create("Joined_" + leftTable.name() + "_" + rightTable.name());
        
        // Add all columns from left table
        for (Column<?> column : leftTable.columns()) {
            joinedTable.addColumns(column.emptyCopy());
        }
        
        // Add all columns from right table (except those with duplicate names)
        for (Column<?> column : rightTable.columns()) {
            String colName = column.name();
            if (!leftTable.columnNames().contains(colName)) {
                joinedTable.addColumns(column.emptyCopy());
            } else {
                // For duplicate column names, add prefix
                String newName = rightTable.name() + "_" + colName;
                Column<?> renamedColumn = column.emptyCopy();
                renamedColumn.setName(newName);
                joinedTable.addColumns(renamedColumn);
            }
        }
        
        // Get date columns
        DateColumn leftDateCol = (DateColumn) leftTable.column(leftColumn);
        DateColumn rightDateCol = (DateColumn) rightTable.column(rightColumn);
        
        // Perform the join
        performInnerJoin(leftTable, rightTable, leftColumn, rightColumn, joinedTable);
        
        // Handle outer joins if needed
        if (joinType == JoinCondition.JoinType.LEFT) {
            addUnmatchedLeftRows(leftTable, rightTable, leftColumn, rightColumn, joinedTable);
        } else if (joinType == JoinCondition.JoinType.RIGHT) {
            addUnmatchedRightRows(leftTable, rightTable, leftColumn, rightColumn, joinedTable);
        }
        
        return joinedTable;
    }
    
    /**
     * Validates that inputs meet the requirements for this join strategy.
     */
    private void validateInputs(Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException {
        
        String leftColumn = joinCondition.leftColumn();
        String rightColumn = joinCondition.rightColumn();
        
        // Check if columns exist
        if (!leftTable.columnNames().contains(leftColumn)) {
            throw new ResultGenerationException(
                "Left join column '" + leftColumn + "' not found in table " + leftTable.name(),
                "naive_contains_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        if (!rightTable.columnNames().contains(rightColumn)) {
            throw new ResultGenerationException(
                "Right join column '" + rightColumn + "' not found in table " + rightTable.name(),
                "naive_contains_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        // Check column types
        if (!(leftTable.column(leftColumn) instanceof DateColumn)) {
            throw new ResultGenerationException(
                "Left join column '" + leftColumn + "' must be a date column",
                "naive_contains_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        if (!(rightTable.column(rightColumn) instanceof DateColumn)) {
            throw new ResultGenerationException(
                "Right join column '" + rightColumn + "' must be a date column",
                "naive_contains_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Performs the inner join part (matching rows from both tables).
     */
    private void performInnerJoin(
            Table leftTable, 
            Table rightTable, 
            String leftColumn, 
            String rightColumn, 
            Table joinedTable) {
        
        // For each row in the left table
        for (Row leftRow : leftTable) {
            LocalDate leftDate = leftRow.getDate(leftColumn);
            
            // Find matching rows in right table
            for (Row rightRow : rightTable) {
                LocalDate rightDate = rightRow.getDate(rightColumn);
                
                // For simple dates, "contains" means equality
                // For date ranges, we would check if left range contains right range
                if (leftDate.isEqual(rightDate)) {
                    // Add a new row with combined values
                    int rowIndex = joinedTable.rowCount();
                    joinedTable.appendRow();
                    
                    // Fill from left row
                    for (String colName : leftTable.columnNames()) {
                        Column<?> targetCol = joinedTable.column(colName);
                        copyValue(leftRow, leftTable.column(colName), joinedTable, targetCol, rowIndex);
                    }
                    
                    // Fill from right row
                    for (String colName : rightTable.columnNames()) {
                        String targetColName = leftTable.columnNames().contains(colName) 
                            ? rightTable.name() + "_" + colName 
                            : colName;
                        Column<?> targetCol = joinedTable.column(targetColName);
                        copyValue(rightRow, rightTable.column(colName), joinedTable, targetCol, rowIndex);
                    }
                }
            }
        }
    }
    
    /**
     * Adds unmatched left rows for LEFT joins.
     */
    private void addUnmatchedLeftRows(
            Table leftTable, 
            Table rightTable, 
            String leftColumn, 
            String rightColumn, 
            Table joinedTable) {
        
        // Track which left rows have been matched already
        Set<Integer> matchedLeftRows = new HashSet<>();
        DateColumn leftDateCol = leftTable.dateColumn(leftColumn);
        DateColumn rightDateCol = rightTable.dateColumn(rightColumn);
        
        // Identify matched rows
        for (int i = 0; i < leftTable.rowCount(); i++) {
            LocalDate leftDate = leftDateCol.get(i);
            
            for (int j = 0; j < rightTable.rowCount(); j++) {
                LocalDate rightDate = rightDateCol.get(j);
                
                if (leftDate.isEqual(rightDate)) {
                    matchedLeftRows.add(i);
                    break;
                }
            }
        }
        
        // Add unmatched left rows
        for (int i = 0; i < leftTable.rowCount(); i++) {
            if (!matchedLeftRows.contains(i)) {
                Row leftRow = leftTable.row(i);
                int rowIndex = joinedTable.rowCount();
                joinedTable.appendRow();
                
                // Fill from left row
                for (String colName : leftTable.columnNames()) {
                    Column<?> targetCol = joinedTable.column(colName);
                    copyValue(leftRow, leftTable.column(colName), joinedTable, targetCol, rowIndex);
                }
                
                // Right columns remain null
            }
        }
    }
    
    /**
     * Adds unmatched right rows for RIGHT joins.
     */
    private void addUnmatchedRightRows(
            Table leftTable, 
            Table rightTable, 
            String leftColumn, 
            String rightColumn, 
            Table joinedTable) {
        
        // Track which right rows have been matched already
        Set<Integer> matchedRightRows = new HashSet<>();
        DateColumn leftDateCol = leftTable.dateColumn(leftColumn);
        DateColumn rightDateCol = rightTable.dateColumn(rightColumn);
        
        // Identify matched rows
        for (int i = 0; i < rightTable.rowCount(); i++) {
            LocalDate rightDate = rightDateCol.get(i);
            
            for (int j = 0; j < leftTable.rowCount(); j++) {
                LocalDate leftDate = leftDateCol.get(j);
                
                if (leftDate.isEqual(rightDate)) {
                    matchedRightRows.add(i);
                    break;
                }
            }
        }
        
        // Add unmatched right rows
        for (int i = 0; i < rightTable.rowCount(); i++) {
            if (!matchedRightRows.contains(i)) {
                Row rightRow = rightTable.row(i);
                int rowIndex = joinedTable.rowCount();
                joinedTable.appendRow();
                
                // Left columns remain null
                
                // Fill from right row
                for (String colName : rightTable.columnNames()) {
                    String targetColName = leftTable.columnNames().contains(colName) 
                        ? rightTable.name() + "_" + colName 
                        : colName;
                    Column<?> targetCol = joinedTable.column(targetColName);
                    copyValue(rightRow, rightTable.column(colName), joinedTable, targetCol, rowIndex);
                }
            }
        }
    }
    
    /**
     * Copies a value from one row/column to another row/column.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void copyValue(Row sourceRow, Column<?> sourceCol, Table targetTable, Column<?> targetCol, int targetRow) {
        if (sourceRow.isMissing(sourceCol.name())) {
            // Skip or set to missing in target
            return;
        }
        
        if (targetCol instanceof DateColumn && sourceCol instanceof DateColumn) {
            ((DateColumn) targetCol).set(targetRow, sourceRow.getDate(sourceCol.name()));
        } else {
            // For other types, try to use the default copy mechanism
            Object value = sourceCol.get(sourceRow.getRowNumber());
            ((Column) targetCol).set(targetRow, value);
        }
    }
} 
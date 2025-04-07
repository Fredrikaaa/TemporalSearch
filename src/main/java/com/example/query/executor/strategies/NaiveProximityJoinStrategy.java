package com.example.query.executor.strategies;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
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
 * A naive implementation of the PROXIMITY temporal join predicate.
 * 
 * This implementation checks if two dates are within a specified 
 * number of days from each other. The proximity window is specified
 * in the join condition.
 */
public class NaiveProximityJoinStrategy implements TemporalJoinStrategy {

    @Override
    public Table execute(Table leftTable, Table rightTable, JoinCondition joinCondition) 
            throws ResultGenerationException {
        
        validateInputs(leftTable, rightTable, joinCondition);
        
        String leftColumn = joinCondition.leftColumn();
        String rightColumn = joinCondition.rightColumn();
        JoinCondition.JoinType joinType = joinCondition.type();
        int window = joinCondition.proximityWindow().orElse(1); // Default to 1 day if not specified
        
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
        
        // Perform the join
        Set<Integer> matchedLeftRows = new HashSet<>();
        Set<Integer> matchedRightRows = new HashSet<>();
        
        performInnerJoin(leftTable, rightTable, leftColumn, rightColumn, joinedTable, window, 
                matchedLeftRows, matchedRightRows);
        
        // Handle outer joins if needed
        if (joinType == JoinCondition.JoinType.LEFT) {
            addUnmatchedLeftRows(leftTable, joinedTable, matchedLeftRows);
        } else if (joinType == JoinCondition.JoinType.RIGHT) {
            addUnmatchedRightRows(leftTable, rightTable, joinedTable, matchedRightRows);
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
                "naive_proximity_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        if (!rightTable.columnNames().contains(rightColumn)) {
            throw new ResultGenerationException(
                "Right join column '" + rightColumn + "' not found in table " + rightTable.name(),
                "naive_proximity_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        // Check column types
        if (!(leftTable.column(leftColumn) instanceof DateColumn)) {
            throw new ResultGenerationException(
                "Left join column '" + leftColumn + "' must be a date column",
                "naive_proximity_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        if (!(rightTable.column(rightColumn) instanceof DateColumn)) {
            throw new ResultGenerationException(
                "Right join column '" + rightColumn + "' must be a date column",
                "naive_proximity_join",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        // PROXIMITY join requires a window parameter
        if (!joinCondition.proximityWindow().isPresent()) {
            throw new ResultGenerationException(
                "PROXIMITY join requires a proximityWindow parameter",
                "naive_proximity_join",
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
            Table joinedTable,
            int window,
            Set<Integer> matchedLeftRows,
            Set<Integer> matchedRightRows) {
        
        DateColumn leftDateCol = leftTable.dateColumn(leftColumn);
        DateColumn rightDateCol = rightTable.dateColumn(rightColumn);
        
        // For each row in the left table
        for (int i = 0; i < leftTable.rowCount(); i++) {
            Row leftRow = leftTable.row(i);
            LocalDate leftDate = leftDateCol.get(i);
            
            // Find matching rows in right table
            for (int j = 0; j < rightTable.rowCount(); j++) {
                Row rightRow = rightTable.row(j);
                LocalDate rightDate = rightDateCol.get(j);
                
                // Check if dates are within the specified window
                long daysDifference = Math.abs(ChronoUnit.DAYS.between(leftDate, rightDate));
                if (daysDifference <= window) {
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
                    
                    // Mark rows as matched for outer joins
                    matchedLeftRows.add(i);
                    matchedRightRows.add(j);
                }
            }
        }
    }
    
    /**
     * Adds unmatched left rows for LEFT joins.
     */
    private void addUnmatchedLeftRows(
            Table leftTable, 
            Table joinedTable,
            Set<Integer> matchedLeftRows) {
        
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
            Table joinedTable,
            Set<Integer> matchedRightRows) {
        
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
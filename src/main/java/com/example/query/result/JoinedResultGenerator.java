package com.example.query.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

import com.example.core.IndexAccess;
import com.example.query.binding.BindingContext;
import com.example.query.executor.SubqueryContext;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.JoinCondition;
import com.example.query.model.Query;
import com.example.query.model.SelectColumn;
import com.example.query.model.SubquerySpec;

/**
 * Specialized result generator for handling joined results from subqueries.
 * Responsible for merging tables and applying projections from both the main query
 * and subqueries based on join conditions.
 */
public class JoinedResultGenerator {
    private static final Logger logger = LoggerFactory.getLogger(JoinedResultGenerator.class);
    
    private final TableResultService tableResultService;
    
    /**
     * Creates a new JoinedResultGenerator.
     * 
     * @param tableResultService The table result service to use
     */
    public JoinedResultGenerator(TableResultService tableResultService) {
        this.tableResultService = tableResultService;
    }
    
    /**
     * Generates a table with joined results from the main query and its subqueries.
     * 
     * @param query The main query
     * @param mainResults The main query results
     * @param bindingContext The binding context from the main query
     * @param subqueryContext The context containing subquery results
     * @param indexes Map of indexes to retrieve additional document information
     * @return A table with the joined results
     * @throws ResultGenerationException if an error occurs
     */
    public Table generateJoinedTable(
            Query query,
            Set<DocSentenceMatch> mainResults,
            BindingContext bindingContext,
            SubqueryContext subqueryContext,
            Map<String, IndexAccess> indexes) 
            throws ResultGenerationException {
        
        // Make sure query has join condition
        if (!query.joinCondition().isPresent() || query.subqueries().isEmpty()) {
            throw new ResultGenerationException(
                "Query does not have join condition or subqueries",
                "joined_result_generator",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        // Generate the main table
        Table mainTable = tableResultService.generateTable(query, mainResults, bindingContext, indexes);
        
        // Get the first subquery for now (in the future, we'll handle multiple joins)
        SubquerySpec subquery = query.subqueries().get(0);
        String subqueryAlias = subquery.alias();
        
        // Make sure subquery table results are available
        Table subqueryTable = subqueryContext.getTableResults(subqueryAlias);
        if (subqueryTable == null) {
            throw new ResultGenerationException(
                "No table results found for subquery: " + subqueryAlias,
                "joined_result_generator",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        // Create column projections by merging both tables
        Table joinedTable = createJoinedTable(mainTable, subqueryTable, subqueryAlias, query.joinCondition().get());
        
        // Apply projections
        joinedTable = applyProjections(joinedTable, query, subquery);
        
        return joinedTable;
    }
    
    /**
     * Creates a joined table from the main table and subquery table.
     * 
     * @param mainTable The main query table
     * @param subqueryTable The subquery table
     * @param subqueryAlias The alias for the subquery
     * @param joinCondition The join condition
     * @return A merged table with columns from both tables
     */
    private Table createJoinedTable(
            Table mainTable, 
            Table subqueryTable, 
            String subqueryAlias,
            JoinCondition joinCondition) {
        
        // Create a new table with columns from both tables
        Table joinedTable = Table.create("Joined_Results");
        
        // Add columns from main table
        for (Column<?> column : mainTable.columns()) {
            joinedTable.addColumns(column.copy());
        }
        
        // Add columns from subquery table with prefix
        for (Column<?> column : subqueryTable.columns()) {
            String originalName = column.name();
            String prefixedName = String.format("%s_%s", subqueryAlias, originalName);
            
            // Skip document_id and sentence_id since they're already in the main table
            if (originalName.equals("document_id") || originalName.equals("sentence_id")) {
                continue;
            }
            
            Column<?> prefixedColumn = column.copy();
            prefixedColumn.setName(prefixedName);
            joinedTable.addColumns(prefixedColumn);
        }
        
        // Only copy the rows from the main table for now
        // In a real implementation, this would perform the actual join
        for (int i = 0; i < mainTable.rowCount(); i++) {
            // Add the row to the joined table
            joinedTable.addRow(mainTable.row(i));
        }
        
        return joinedTable;
    }
    
    /**
     * Applies projections to the joined table based on the SELECT columns.
     * 
     * @param joinedTable The joined table
     * @param query The main query
     * @param subquery The subquery
     * @return The table with projections applied
     */
    private Table applyProjections(Table joinedTable, Query query, SubquerySpec subquery) {
        List<String> columnsToKeep = new ArrayList<>();
        
        // Always keep document_id and sentence_id
        columnsToKeep.add("document_id");
        if (joinedTable.columnNames().contains("sentence_id")) {
            columnsToKeep.add("sentence_id");
        }
        
        // Add projected columns from main query
        for (SelectColumn selectColumn : query.selectColumns()) {
            String columnName = selectColumn.getColumnName();
            if (joinedTable.columnNames().contains(columnName)) {
                columnsToKeep.add(columnName);
            }
        }
        
        // Add projected columns from subquery
        if (subquery.projectedColumns().isPresent()) {
            for (String columnName : subquery.projectedColumns().get()) {
                String prefixedName = String.format("%s_%s", subquery.alias(), columnName);
                if (joinedTable.columnNames().contains(prefixedName)) {
                    columnsToKeep.add(prefixedName);
                }
            }
        } else {
            // If no projections specified, include all subquery columns
            String prefix = subquery.alias() + "_";
            for (String columnName : joinedTable.columnNames()) {
                if (columnName.startsWith(prefix)) {
                    columnsToKeep.add(columnName);
                }
            }
        }
        
        // If no columns to keep other than document_id and sentence_id, keep all columns
        if (columnsToKeep.size() <= 2) {
            return joinedTable;
        }
        
        // Return projected table
        return joinedTable.selectColumns(columnsToKeep.toArray(new String[0]));
    }
} 
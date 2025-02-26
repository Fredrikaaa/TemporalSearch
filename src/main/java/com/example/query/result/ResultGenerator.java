package com.example.query.result;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.model.OrderSpec;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates result tables from query results.
 * Responsible for creating table structure, populating rows, and applying ordering and limits.
 */
public class ResultGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ResultGenerator.class);
    
    /**
     * Generates a result table from query results
     *
     * @param query The original query
     * @param documentIds Set of matching document IDs
     * @param variableBindings Variable bindings from query execution
     * @param indexes Map of indexes to retrieve additional document information
     * @return Result table containing query results
     * @throws ResultGenerationException if an error occurs
     */
    public ResultTable generateResultTable(
        Query query,
        Set<Integer> documentIds,
        VariableBindings variableBindings,
        Map<String, IndexAccess> indexes
    ) throws ResultGenerationException {
        logger.debug("Generating result table for {} matching documents", documentIds.size());
        
        try {
            // Create column specifications
            List<ColumnSpec> columns = createColumnSpecs(query);
            
            // Create result rows
            List<Map<String, String>> rows = createResultRows(
                query, documentIds, variableBindings, indexes, columns);
            
            // Create the result table
            ResultTable resultTable = new ResultTable(columns, rows);
            
            // Apply ordering if specified
            if (!query.getOrderBy().isEmpty()) {
                // Sort the rows based on order specifications
                // This would be implemented in the ResultTable class in a real implementation
                logger.debug("Ordering results by {} criteria", query.getOrderBy().size());
                // resultTable.applyOrdering(query.getOrderBy());
            }
            
            // Apply limit if specified
            if (query.getLimit().isPresent()) {
                int limit = query.getLimit().get();
                logger.debug("Limiting results to {} rows", limit);
                
                // Limit the number of rows
                // This would be implemented in the ResultTable class in a real implementation
                if (rows.size() > limit) {
                    List<Map<String, String>> limitedRows = rows.subList(0, limit);
                    resultTable = new ResultTable(columns, limitedRows);
                }
            }
            
            logger.debug("Generated result table with {} columns and {} rows", 
                    columns.size(), resultTable.getRowCount());
            
            return resultTable;
        } catch (Exception e) {
            throw new ResultGenerationException(
                "Failed to generate result table: " + e.getMessage(),
                "result_generator",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
    }
    
    /**
     * Creates column specifications based on the query.
     *
     * @param query The query
     * @return List of column specifications
     */
    private List<ColumnSpec> createColumnSpecs(Query query) {
        List<ColumnSpec> columns = new ArrayList<>();
        
        // Always include document ID column
        columns.add(new ColumnSpec("document_id", ColumnType.TERM));
        
        // TODO: Add columns based on query select list
        // For now, we'll just add a basic set of columns
        
        return columns;
    }
    
    /**
     * Creates result rows from matching document IDs.
     *
     * @param query The query
     * @param documentIds Matching document IDs
     * @param variableBindings Variable bindings
     * @param indexes Available indexes
     * @param columns Column specifications
     * @return List of result rows
     * @throws ResultGenerationException if an error occurs
     */
    private List<Map<String, String>> createResultRows(
        Query query,
        Set<Integer> documentIds,
        VariableBindings variableBindings,
        Map<String, IndexAccess> indexes,
        List<ColumnSpec> columns
    ) throws ResultGenerationException {
        List<Map<String, String>> rows = new ArrayList<>();
        
        for (Integer docId : documentIds) {
            Map<String, String> row = new HashMap<>();
            
            // Add document ID
            row.put("document_id", docId.toString());
            
            // Add variable bindings for this document
            Map<String, String> docBindings = variableBindings.getBindingsForDocument(docId);
            for (Map.Entry<String, String> binding : docBindings.entrySet()) {
                row.put(binding.getKey(), binding.getValue());
            }
            
            // TODO: Add additional document metadata
            
            rows.add(row);
        }
        
        return rows;
    }
} 
package com.example.query.result;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.model.OrderSpec;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.query.snippet.ContextAnchor;
import com.example.query.snippet.SnippetConfig;
import com.example.query.snippet.SnippetGenerator;
import com.example.query.snippet.TableSnippet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
    private final SnippetConfig snippetConfig;
    private Connection dbConnection;
    
    /**
     * Creates a new ResultGenerator with default snippet configuration.
     */
    public ResultGenerator() {
        this.snippetConfig = SnippetConfig.DEFAULT;
    }
    
    /**
     * Creates a new ResultGenerator with custom snippet configuration.
     * 
     * @param snippetConfig The snippet configuration to use
     */
    public ResultGenerator(SnippetConfig snippetConfig) {
        this.snippetConfig = snippetConfig;
    }
    
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
            List<ColumnSpec> columns = createColumnSpecs(query, variableBindings, documentIds);
            
            // Create result rows
            List<Map<String, String>> rows = createResultRows(
                query, documentIds, variableBindings, indexes, columns);
            
            // Create the result table
            ResultTable resultTable = new ResultTable(columns, rows);
            
            // Apply ordering if specified
            if (!query.getOrderBy().isEmpty()) {
                // Sort the rows based on order specifications
                logger.debug("Ordering results by {} criteria", query.getOrderBy().size());
                resultTable = resultTable.sort(query.getOrderBy());
            }
            
            // Apply limit if specified
            if (query.getLimit().isPresent()) {
                int limit = query.getLimit().get();
                logger.debug("Limiting results to {} rows", limit);
                resultTable = resultTable.limit(limit);
            }
            
            logger.debug("Generated result table with {} columns and {} rows", 
                    columns.size(), resultTable.getRowCount());
            
            return resultTable;
        } catch (Exception e) {
            throw new ResultGenerationException(
                "Failed to generate result table: " + e.getMessage(),
                e,
                "result_generator",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        } finally {
            closeDbConnection();
        }
    }
    
    /**
     * Creates column specifications based on the query and available variable bindings.
     *
     * @param query The query
     * @param variableBindings Variable bindings from query execution
     * @param documentIds Set of matching document IDs
     * @return List of column specifications
     */
    private List<ColumnSpec> createColumnSpecs(
        Query query, 
        VariableBindings variableBindings,
        Set<Integer> documentIds
    ) {
        List<ColumnSpec> columns = new ArrayList<>();
        
        // Always include document ID column
        columns.add(new ColumnSpec("document_id", ColumnType.TERM));
        
        // Add columns for all variables that have bindings
        Set<String> boundVariables = new HashSet<>();
        
        // Collect all variable names from all documents
        for (Integer docId : documentIds) {
            Map<String, String> docBindings = variableBindings.getBindingsForDocument(docId);
            boundVariables.addAll(docBindings.keySet());
        }
        
        logger.debug("Adding columns for {} bound variables", boundVariables.size());
        
        for (String variable : boundVariables) {
            columns.add(new ColumnSpec(variable, ColumnType.TERM));
            
            // Add a snippet column for each variable
            Map<String, String> options = new HashMap<>();
            options.put("window_size", String.valueOf(snippetConfig.windowSize()));
            options.put("highlight", "true");
            
            columns.add(new ColumnSpec(
                variable + "_snippet",
                ColumnType.SNIPPET,
                options
            ));
        }
        
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
        
        try {
            // Initialize database connection for snippet generation if needed
            initDbConnection(query.getSource());
            
            // Create snippet generator if we have a DB connection
            SnippetGenerator snippetGenerator = dbConnection != null ? 
                new SnippetGenerator(dbConnection, snippetConfig) : null;
            
            for (Integer docId : documentIds) {
                Map<String, String> row = new HashMap<>();
                
                // Add document ID
                row.put("document_id", docId.toString());
                
                // Add variable bindings for this document
                Map<String, String> docBindings = variableBindings.getBindingsForDocument(docId);
                for (Map.Entry<String, String> binding : docBindings.entrySet()) {
                    String variableName = binding.getKey();
                    String value = binding.getValue();
                    
                    // Add the variable value
                    row.put(variableName, value);
                    
                    // Generate snippet if we have a snippet generator
                    if (snippetGenerator != null) {
                        try {
                            // Parse the value to get token position and sentence ID
                            // Format is typically "value@sentenceId:tokenPosition"
                            String[] parts = value.split("@");
                            if (parts.length > 1) {
                                String[] locationParts = parts[1].split(":");
                                if (locationParts.length == 2) {
                                    int sentenceId = Integer.parseInt(locationParts[0]);
                                    int tokenPosition = Integer.parseInt(locationParts[1]);
                                    
                                    // Create anchor and generate snippet
                                    ContextAnchor anchor = new ContextAnchor(docId, sentenceId, tokenPosition, variableName);
                                    TableSnippet snippet = snippetGenerator.generateSnippet(anchor);
                                    
                                    // Format and add to row
                                    String formattedSnippet = snippetGenerator.formatSnippet(snippet);
                                    row.put(variableName + "_snippet", formattedSnippet);
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Failed to generate snippet for variable {} in document {}: {}", 
                                variableName, docId, e.getMessage());
                            row.put(variableName + "_snippet", "[Snippet generation failed]");
                        }
                    } else {
                        // No snippet generator available
                        row.put(variableName + "_snippet", "[Snippets not available]");
                    }
                }
                
                // Add document metadata if available
                try {
                    if (indexes.containsKey("metadata")) {
                        IndexAccess metadataIndex = indexes.get("metadata");
                        String docIdStr = docId.toString();
                        
                        // In a real implementation, we would retrieve and parse metadata
                        // For now, just indicate that metadata is available
                        row.put("metadata_available", "true");
                    }
                } catch (Exception e) {
                    logger.warn("Failed to retrieve metadata for document {}: {}", 
                        docId, e.getMessage());
                    row.put("metadata_available", "false");
                }
                
                rows.add(row);
            }
        } catch (Exception e) {
            throw new ResultGenerationException(
                "Failed to create result rows: " + e.getMessage(),
                e,
                "result_generator",
                ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        }
        
        return rows;
    }
    
    /**
     * Initializes the database connection for snippet generation.
     * 
     * @param source The source database name from the query
     */
    private void initDbConnection(String source) {
        if (dbConnection != null) {
            return;  // Already initialized
        }
        
        try {
            // In a real implementation, we would use the source to determine the database path
            // For now, we'll just use a placeholder
            String dbPath = source != null ? source : "documents.db";
            String jdbcUrl = "jdbc:sqlite:" + dbPath;
            
            logger.debug("Initializing database connection to {}", jdbcUrl);
            dbConnection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            logger.warn("Failed to initialize database connection: {}", e.getMessage());
            // We'll continue without a database connection, which means no snippets
        }
    }
    
    /**
     * Closes the database connection if open.
     */
    private void closeDbConnection() {
        if (dbConnection != null) {
            try {
                dbConnection.close();
                dbConnection = null;
            } catch (SQLException e) {
                logger.warn("Failed to close database connection: {}", e.getMessage());
            }
        }
    }
} 
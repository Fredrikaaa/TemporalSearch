package com.example.query.result;

import com.example.core.IndexAccess;
import com.example.query.executor.VariableBindings;
import com.example.query.model.CountNode;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.model.SelectColumn;
import com.example.query.model.MetadataColumn;
import com.example.query.model.SnippetColumn;
import com.example.query.model.SnippetNode;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Ner;
import com.example.query.snippet.DatabaseConfig;
import com.example.query.snippet.SnippetConfig;
import com.example.query.snippet.SnippetGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final String dbPath;
    
    /**
     * Creates a new ResultGenerator with default snippet configuration and database path.
     */
    public ResultGenerator() {
        this.snippetConfig = SnippetConfig.DEFAULT;
        this.dbPath = DatabaseConfig.DEFAULT_DB_PATH;
    }
    
    /**
     * Creates a new ResultGenerator with custom snippet configuration.
     * 
     * @param snippetConfig The snippet configuration to use
     */
    public ResultGenerator(SnippetConfig snippetConfig) {
        this.snippetConfig = snippetConfig;
        this.dbPath = DatabaseConfig.DEFAULT_DB_PATH;
    }
    
    /**
     * Creates a new ResultGenerator with custom snippet configuration and database path.
     * 
     * @param snippetConfig The snippet configuration to use
     * @param dbPath The path to the database file
     */
    public ResultGenerator(SnippetConfig snippetConfig, String dbPath) {
        this.snippetConfig = snippetConfig;
        this.dbPath = dbPath;
    }
    
    /**
     * Creates a new ResultGenerator with default snippet configuration and custom database path.
     * 
     * @param dbPath The path to the database file
     */
    public ResultGenerator(String dbPath) {
        this.snippetConfig = SnippetConfig.DEFAULT;
        this.dbPath = dbPath;
    }
    
    /**
     * Generates a result table from query results
     *
     * @param query The original query
     * @param matches Set of matching document/sentence matches
     * @param variableBindings Variable bindings from query execution
     * @param indexes Map of indexes to retrieve additional document information
     * @return Result table containing query results
     * @throws ResultGenerationException if an error occurs
     */
    public ResultTable generateResultTable(
        Query query,
        Set<DocSentenceMatch> matches,
        VariableBindings variableBindings,
        Map<String, IndexAccess> indexes
    ) throws ResultGenerationException {
        Query.Granularity granularity = query.granularity();
        logger.debug("Generating result table for {} matching {} at {} granularity", 
                matches.size(), 
                granularity == Query.Granularity.DOCUMENT ? "documents" : "sentences",
                granularity);
        
        try {
            // Check if this is a COUNT query
            List<SelectColumn> selectColumns = query.selectColumns();
            if (selectColumns != null && !selectColumns.isEmpty()) {
                // Check for COUNT expressions
                for (SelectColumn column : selectColumns) {
                    if (column instanceof CountNode countNode) {
                        return generateCountResultTable(countNode, matches, variableBindings);
                    }
                }
            }
            
            // Create column specifications
            List<ColumnSpec> columns = createColumnSpecs(query, variableBindings, matches);
            
            // Create result rows
            List<Map<String, String>> rows = createResultRows(
                query, matches, variableBindings, indexes, columns);
            
            // Create the result table
            ResultTable resultTable = new ResultTable(columns, rows);
            
            // Apply ordering if specified
            if (!query.orderBy().isEmpty()) {
                // Sort the rows based on order specifications
                logger.debug("Ordering results by {} criteria", query.orderBy().size());
                
                // Use the string-based order columns directly
                resultTable = resultTable.sort(query.orderBy());
            }
            
            // Apply limit if specified
            if (query.limit().isPresent()) {
                int limit = query.limit().get();
                logger.debug("Limiting results to {} rows out of {}", limit, resultTable.getRows().size());
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
     * @param matches Set of matching document/sentence matches
     * @return List of column specifications
     */
    private List<ColumnSpec> createColumnSpecs(
        Query query, 
        VariableBindings variableBindings,
        Set<DocSentenceMatch> matches
    ) {
        List<ColumnSpec> columns = new ArrayList<>();
        Query.Granularity granularity = query.granularity();
        
        // Always include document ID column
        columns.add(new ColumnSpec("document_id", ColumnType.TERM));
        
        // Include sentence ID column for sentence granularity
        if (granularity == Query.Granularity.SENTENCE) {
            columns.add(new ColumnSpec("sentence_id", ColumnType.TERM));
        }
        
        // Check if we have a METADATA column in the SELECT clause
        List<SelectColumn> selectColumns = query.selectColumns();
        if (selectColumns != null) {
            for (SelectColumn column : selectColumns) {
                if (column instanceof MetadataColumn) {
                    MetadataColumn metadataColumn = (MetadataColumn) column;
                    if (metadataColumn.selectsAllFields()) {
                        // Add all metadata fields without the metadata_ prefix
                        columns.add(new ColumnSpec("title", ColumnType.TERM));
                        columns.add(new ColumnSpec("timestamp", ColumnType.TERM));
                    } else if (metadataColumn.getFieldName() != null) {
                        // Add specific metadata field without the metadata_ prefix
                        columns.add(new ColumnSpec(metadataColumn.getFieldName(), ColumnType.TERM));
                    }
                }
            }
        }
        
        // Add columns for all variable bindings
        Set<Integer> documentIds = matches.stream()
                .map(DocSentenceMatch::documentId)
                .collect(Collectors.toSet());
        
        Set<String> variableNames = new HashSet<>();
        for (int docId : documentIds) {
            variableNames.addAll(variableBindings.getValuesForDocument(docId).keySet());
            
            // Also check sentence-level bindings if we're at sentence granularity
            if (granularity == Query.Granularity.SENTENCE) {
                // Get all sentences for this document from the matches
                Set<Integer> sentenceIds = matches.stream()
                        .filter(match -> match.documentId() == docId)
                        .map(DocSentenceMatch::sentenceId)
                        .collect(Collectors.toSet());
                
                // Add variable names from sentence-level bindings
                for (int sentId : sentenceIds) {
                    Map<String, List<String>> sentBindings = variableBindings.getValuesForSentence(docId, sentId);
                    variableNames.addAll(sentBindings.keySet());
                    logger.debug("Found sentence-level variable names for doc {} sent {}: {}", 
                                 docId, sentId, sentBindings.keySet());
                }
            }
        }
        
        for (String variableName : variableNames) {
            columns.add(new ColumnSpec(variableName, ColumnType.TERM));
        }
        
        // Add snippet columns if specified
        if (selectColumns != null) {
            for (SelectColumn column : selectColumns) {
                if (column instanceof SnippetColumn) {
                    SnippetColumn snippetColumn = (SnippetColumn) column;
                    String variableName = snippetColumn.getSnippetNode().variable();
                    if (variableName.startsWith("?")) {
                        variableName = variableName.substring(1);
                    }
                    columns.add(new ColumnSpec("snippet_" + variableName, ColumnType.SNIPPET));
                }
            }
        }
        
        return columns;
    }
    
    /**
     * Creates result rows from query results.
     *
     * @param query The query
     * @param matches Set of matching document/sentence matches
     * @param variableBindings Variable bindings from query execution
     * @param indexes Map of indexes to retrieve additional document information
     * @param columns Column specifications
     * @return List of result rows
     * @throws ResultGenerationException if an error occurs
     */
    private List<Map<String, String>> createResultRows(
        Query query,
        Set<DocSentenceMatch> matches,
        VariableBindings variableBindings,
        Map<String, IndexAccess> indexes,
        List<ColumnSpec> columns
    ) throws ResultGenerationException {
        List<Map<String, String>> rows = new ArrayList<>();
        Query.Granularity granularity = query.granularity();
        
        // Extract query variables from SELECT clause and WHERE conditions
        List<String> queryVariables = extractQueryVariables(query);
        logger.debug("Query variables in SELECT: {}", queryVariables);
        
        // Process each match
        for (DocSentenceMatch match : matches) {
            int documentId = match.documentId();
            int sentenceId = match.sentenceId();
            
            logger.debug("Processing match for documentId={}, sentenceId={}", documentId, sentenceId);

            // Get all variable values for this document/sentence
            Map<String, List<String>> allValues = variableBindings.getAllValuesForSentence(documentId, sentenceId);
            
            logger.debug("Variable values for doc {} sent {}: {}", documentId, sentenceId, allValues);
            
            if (allValues.isEmpty()) {
                // No variable bindings for this match, create empty row
                Map<String, String> row = createBaseRow(query, match, granularity);
                rows.add(row);
                continue;
            }
            
            // Focus on variables that are specifically selected in the query
            if (!queryVariables.isEmpty()) {
                // Create a map of all values for query variables
                Map<String, List<String>> selectedValues = new HashMap<>();
                
                for (String varName : queryVariables) {
                    List<String> values = allValues.getOrDefault(varName, Collections.emptyList());
                    selectedValues.put(varName, values);
                }
                
                if (selectedValues.isEmpty() || selectedValues.values().stream().allMatch(List::isEmpty)) {
                    // No selected variables have bindings, create empty row
                    Map<String, String> row = createBaseRow(query, match, granularity);
                    // Add empty values for selected variables
                    for (String varName : queryVariables) {
                        row.put(varName, "");
                    }
                    rows.add(row);
                    continue;
                }
                
                // Determine how many rows we need to create for this document/sentence
                // by finding the maximum number of values for any of the variables
                int maxBindings = 0;
                for (List<String> values : selectedValues.values()) {
                    maxBindings = Math.max(maxBindings, values.size());
                }
                
                // Create one row for each binding
                for (int i = 0; i < maxBindings; i++) {
                    Map<String, String> row = createBaseRow(query, match, granularity);
                    
                    // Add variable values
                    for (Map.Entry<String, List<String>> entry : selectedValues.entrySet()) {
                        String varName = entry.getKey();
                        List<String> values = entry.getValue();
                        
                        if (i < values.size()) {
                            // We have a value for this position
                            row.put(varName, values.get(i));
                        }
                    }
                    
                    // Add snippets if requested
                    addSnippetsToRow(query, match, variableBindings, row);
                    
                    rows.add(row);
                }
            } else {
                // No specific variables selected, create a single row with the first value of each variable
                Map<String, String> row = createBaseRow(query, match, granularity);
                
                // Add first value of each variable
                for (Map.Entry<String, List<String>> entry : allValues.entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        row.put(entry.getKey(), entry.getValue().get(0));
                    }
                }
                
                // Add snippets if requested
                addSnippetsToRow(query, match, variableBindings, row);
                
                rows.add(row);
            }
        }
        
        return rows;
    }
    
    /**
     * Creates a base row with document ID, sentence ID (if applicable), and metadata.
     * 
     * @param query The query
     * @param match The document/sentence match
     * @param granularity The query granularity
     * @return A new row with base information
     * @throws ResultGenerationException if metadata access fails
     */
    private Map<String, String> createBaseRow(Query query, DocSentenceMatch match, Query.Granularity granularity) 
            throws ResultGenerationException {
        Map<String, String> row = new HashMap<>();
        
        // Add document ID
        row.put("document_id", String.valueOf(match.documentId()));
        
        // Add sentence ID for sentence granularity
        if (granularity == Query.Granularity.SENTENCE) {
            row.put("sentence_id", String.valueOf(match.sentenceId()));
        }
        
        // Add metadata if requested
        addMetadataToRow(query, match.documentId(), row);
        
        return row;
    }
    
    /**
     * Extract variable names from the SELECT clause and WHERE conditions.
     * 
     * @param query The query
     * @return List of variable names (without the ? prefix)
     */
    private List<String> extractQueryVariables(Query query) {
        Set<String> variables = new HashSet<>();
        List<SelectColumn> selectColumns = query.selectColumns();
        
        // Extract variables from SELECT clause
        if (selectColumns != null) {
            for (SelectColumn column : selectColumns) {
                String colString = column.toString();
                if (colString.startsWith("?")) {
                    // Variable column, strip the leading ?
                    String varName = colString.substring(1);
                    variables.add(varName);
                } else if (column instanceof SnippetColumn) {
                    // Snippet column, extract the variable
                    SnippetNode snippetNode = ((SnippetColumn) column).getSnippetNode();
                    String varName = snippetNode.variable();
                    if (varName.startsWith("?")) {
                        varName = varName.substring(1);
                    }
                    variables.add(varName);
                }
            }
        }
        
        // Also extract variables from NER conditions, regardless of whether we found variables in SELECT
        // This ensures variables that only appear in WHERE clauses are also included
        logger.debug("Checking for variables in conditions");
        for (Condition condition : query.conditions()) {
            logger.debug("Checking condition: {}", condition);
            if (condition instanceof Ner) {
                Ner nerCondition = (Ner) condition;
                String varName = nerCondition.variableName();
                logger.debug("Found NER condition: {} with variable: {}, isVariable: {}", 
                             nerCondition.entityType(), varName, nerCondition.isVariable());
                if (nerCondition.isVariable()) {
                    if (varName.startsWith("?")) {
                        varName = varName.substring(1);
                    }
                    variables.add(varName);
                    logger.debug("Added variable from NER condition: {}", varName);
                }
            }
        }
        
        logger.debug("Extracted variables: {}", variables);
        return new ArrayList<>(variables);
    }
    
    /**
     * Generates a COUNT result table.
     *
     * @param countNode The COUNT node from the query
     * @param matches Set of matching document/sentence matches
     * @param variableBindings Variable bindings from query execution
     * @return Result table with COUNT results
     */
    private ResultTable generateCountResultTable(
        CountNode countNode,
        Set<DocSentenceMatch> matches,
        VariableBindings variableBindings
    ) {
        logger.debug("Generating COUNT result table");
        
        // Create column specifications
        List<ColumnSpec> columns = new ArrayList<>();
        columns.add(new ColumnSpec("count", ColumnType.TERM));
        
        // Create a single row with the count
        List<Map<String, String>> rows = new ArrayList<>();
        Map<String, String> row = new HashMap<>();
        row.put("count", String.valueOf(matches.size()));
        rows.add(row);
        
        return new ResultTable(columns, rows);
    }
    
    /**
     * Adds metadata fields to a result row.
     *
     * @param query The query
     * @param documentId Document ID
     * @param row The row to add metadata to
     * @throws ResultGenerationException if an error occurs
     */
    private void addMetadataToRow(Query query, int documentId, Map<String, String> row) 
            throws ResultGenerationException {
        // Check if we have metadata columns in the SELECT clause
        List<SelectColumn> selectColumns = query.selectColumns();
        if (selectColumns == null || selectColumns.isEmpty()) {
            return;
        }
        
        boolean hasMetadataColumn = false;
        Set<String> specificFields = new HashSet<>();
        
        // Check for metadata columns and collect specific fields if any
        for (SelectColumn column : selectColumns) {
            if (column instanceof MetadataColumn) {
                MetadataColumn metadataColumn = (MetadataColumn) column;
                hasMetadataColumn = true;
                
                if (!metadataColumn.selectsAllFields() && metadataColumn.getFieldName() != null) {
                    specificFields.add(metadataColumn.getFieldName());
                }
            }
        }
        
        // If no metadata columns, return early
        if (!hasMetadataColumn) {
            return;
        }
        
        try {
            // Initialize database connection if needed
            initDbConnection(dbPath);
            
            // If connection failed, log warning and return
            if (dbConnection == null) {
                logger.warn("Cannot add metadata: database connection is null");
                return;
            }
            
            // Build query based on whether we need all fields or specific ones
            String sql;
            if (specificFields.isEmpty()) {
                // Select all metadata fields
                sql = "SELECT title, timestamp FROM documents WHERE document_id = ?";
            } else {
                // Select only specific fields
                StringBuilder sqlBuilder = new StringBuilder("SELECT ");
                for (String field : specificFields) {
                    sqlBuilder.append(field).append(", ");
                }
                // Remove trailing comma and space
                sqlBuilder.setLength(sqlBuilder.length() - 2);
                sqlBuilder.append(" FROM documents WHERE document_id = ?");
                sql = sqlBuilder.toString();
            }
            
            try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
                stmt.setInt(1, documentId);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // If selecting all fields
                        if (specificFields.isEmpty()) {
                            String title = rs.getString("title");
                            String timestamp = rs.getString("timestamp");
                            
                            if (title != null) {
                                row.put("title", title);
                            }
                            
                            if (timestamp != null) {
                                row.put("timestamp", timestamp);
                            }
                        } else {
                            // If selecting specific fields
                            for (String field : specificFields) {
                                String value = rs.getString(field);
                                if (value != null) {
                                    row.put(field, value);
                                }
                            }
                        }
                    } else {
                        logger.warn("No metadata found for document ID: {}", documentId);
                    }
                }
            }
        } catch (SQLException e) {
            throw new ResultGenerationException(
                "Failed to retrieve metadata for document " + documentId + ": " + e.getMessage(),
                e,
                "metadata_retrieval",
                ResultGenerationException.ErrorType.METADATA_ACCESS_ERROR
            );
        }
    }
    
    /**
     * Adds snippets to a result row.
     *
     * @param query The query
     * @param match The document/sentence match
     * @param variableBindings Variable bindings from query execution
     * @param row The row to add snippets to
     * @throws ResultGenerationException if an error occurs
     */
    private void addSnippetsToRow(
        Query query,
        DocSentenceMatch match,
        VariableBindings variableBindings,
        Map<String, String> row
    ) throws ResultGenerationException {
        logger.debug("Adding snippets for documentId={}, sentenceId={}", match.documentId(), match.sentenceId());
        
        // Don't process snippets if we're beyond the LIMIT
        Optional<Integer> limitOpt = query.limit();
        
        if (limitOpt.isPresent() && row.containsKey("_row_num")) {
            int rowNum = Integer.parseInt(row.get("_row_num"));
            if (rowNum > limitOpt.get()) {
                logger.debug("Skipping snippet generation for row {} (beyond LIMIT {})", rowNum, limitOpt.get());
                return;
            }
        }
        
        try {
            // Initialize DB connection if needed
            if (dbConnection == null || dbConnection.isClosed()) {
                initDbConnection(dbPath);
            }
            
            // Find SnippetNode instances in the query
            List<SnippetNode> snippetNodes = new ArrayList<>();
            
            for (SelectColumn column : query.selectColumns()) {
                if (column instanceof SnippetColumn) {
                    SnippetNode snippetNode = ((SnippetColumn) column).getSnippetNode();
                    snippetNodes.add(snippetNode);
                } else if (column instanceof SnippetNode) {
                    snippetNodes.add((SnippetNode) column);
                }
            }
            
            // Process snippets in batch
            SnippetGenerator snippetGenerator = new SnippetGenerator(
                dbConnection, 
                snippetConfig.windowSize(),
                snippetConfig.highlightStyle(),
                snippetConfig.showSentenceBoundaries()
            );
            
            // Process each snippet node
            for (SnippetNode snippetNode : snippetNodes) {
                String variableName = snippetNode.variable();
                
                // Remove leading ? if present
                if (variableName.startsWith("?")) {
                    variableName = variableName.substring(1);
                }
                
                // Get column name for output
                String columnName = "snippet_" + variableName;
                
                try {
                    // Check if we have this variable in the current row
                    if (!row.containsKey(variableName)) {
                        logger.warn("Variable '{}' not found in current row", variableName);
                        row.put(columnName, "");
                        continue;
                    }
                    
                    // Extract positions from the variable value in the current row
                    String valueStr = row.get(variableName);
                    int beginPos = -1;
                    int endPos = -1;
                    
                    // Parse the position from the value format: term@beginPos:endPos
                    int atPos = valueStr.lastIndexOf('@');
                    if (atPos != -1) {
                        int colonPos = valueStr.lastIndexOf(':');
                        if (colonPos != -1 && colonPos > atPos) {
                            try {
                                beginPos = Integer.parseInt(valueStr.substring(atPos + 1, colonPos));
                                endPos = Integer.parseInt(valueStr.substring(colonPos + 1));
                            } catch (NumberFormatException e) {
                                logger.warn("Could not parse positions from value: {}", valueStr);
                            }
                        }
                    }
                    
                    // If we couldn't extract positions from the row value, fall back to the variable bindings method
                    if (beginPos == -1 || endPos == -1) {
                        beginPos = variableBindings.getBeginCharPosition(variableName, match);
                        endPos = variableBindings.getEndCharPosition(variableName, match);
                    }
                    
                    if (beginPos == -1 || endPos == -1) {
                        logger.warn("Could not find character positions for variable '{}'", variableName);
                        row.put(columnName, "");
                        continue;
                    }
                    
                    // Generate the snippet with begin and end positions
                    String snippetText = snippetGenerator.generateSnippet(
                        match.documentId(),
                        match.sentenceId(),
                        beginPos,
                        endPos,
                        variableName
                    );
                    
                    row.put(columnName, snippetText);
                    logger.debug("Added snippet for variable '{}': {}", variableName, 
                        snippetText.length() > 50 ? snippetText.substring(0, 50) + "..." : snippetText);
                    
                } catch (Exception e) {
                    logger.error("Error generating snippet for variable {}: {}", variableName, e.getMessage());
                    row.put(columnName, "");
                }
            }
            
            // Clear snippet generator cache to save memory
            snippetGenerator.clearCache();
            
        } catch (SQLException e) {
            logger.error("Database error while generating snippets: {}", e.getMessage());
            throw new ResultGenerationException(
                "Error connecting to database for snippets: " + e.getMessage(),
                "SnippetGenerator",
                ResultGenerationException.ErrorType.SNIPPET_GENERATION_ERROR
            );
        }
    }
    
    /**
     * Initializes the database connection.
     *
     * @param dbPath Path to the database file
     * @throws ResultGenerationException if connection fails
     */
    private void initDbConnection(String dbPath) throws ResultGenerationException {
        if (dbConnection != null) {
            return;
        }
        
        try {
            File dbFile = new File(dbPath);
            if (!dbFile.exists()) {
                logger.warn("Database file not found: {}", dbPath);
                return;
            }
            
            String url = "jdbc:sqlite:" + dbPath;
            dbConnection = DriverManager.getConnection(url);
            logger.debug("Connected to database: {}", dbPath);
        } catch (SQLException e) {
            throw new ResultGenerationException(
                "Failed to connect to database: " + e.getMessage(),
                e,
                "database_connection",
                ResultGenerationException.ErrorType.METADATA_ACCESS_ERROR
            );
        }
    }
    
    /**
     * Closes the database connection.
     */
    private void closeDbConnection() {
        if (dbConnection != null) {
            try {
                dbConnection.close();
                dbConnection = null;
            } catch (SQLException e) {
                logger.warn("Error closing database connection: {}", e.getMessage());
            }
        }
    }
    
    private void addVariableBindingsToRow(
        Query query,
        DocSentenceMatch match,
        VariableBindings variableBindings,
        Map<String, String> row
    ) {
        List<String> variables = extractQueryVariables(query);
        logger.debug("Adding variable bindings for variables: {} to row for match: {}", variables, match);
        
        for (String variable : variables) {
            // Check for both doc-level and sentence-level bindings
            List<String> values = new ArrayList<>();
            
            // Try document-level bindings first
            List<String> docBindings = variableBindings.getValues(match.documentId(), variable);
            if (docBindings != null && !docBindings.isEmpty()) {
                logger.debug("Found document-level bindings for variable {} in doc {}: {}", 
                             variable, match.documentId(), docBindings);
                values.addAll(docBindings);
            }
            
            // Then try sentence-level bindings if applicable
            if (match.isSentenceLevel()) {
                List<String> sentBindings = variableBindings.getValues(
                        match.documentId(), match.sentenceId(), variable);
                if (sentBindings != null && !sentBindings.isEmpty()) {
                    logger.debug("Found sentence-level bindings for variable {} in doc {} sentence {}: {}", 
                                 variable, match.documentId(), match.sentenceId(), sentBindings);
                    values.addAll(sentBindings);
                }
            }
            
            // Always add the variable to the row, even if no values found
            String value = values.isEmpty() ? "" : String.join(", ", values);
            logger.debug("Adding variable {} with value '{}' to row", variable, value);
            row.put(variable, value);
        }
    }
} 
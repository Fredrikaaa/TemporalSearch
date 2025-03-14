package com.example.query.result;

import com.example.query.executor.VariableBindings;
import com.example.query.model.CountNode;
import com.example.query.model.CountColumn;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.MetadataColumn;
import com.example.query.model.OrderSpec;
import com.example.query.model.Query;
import com.example.query.model.SelectColumn;
import com.example.query.model.SnippetColumn;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.core.IndexAccess;
import com.example.query.snippet.DatabaseConfig;
import com.example.query.snippet.SnippetConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvWriteOptions;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Service for converting query results to Tablesaw Tables.
 * Replaces the previous ResultGenerator with a simpler implementation
 * that leverages Tablesaw for data representation and formatting.
 */
public class TableResultService {
    private static final Logger logger = LoggerFactory.getLogger(TableResultService.class);
    private final SnippetConfig snippetConfig;
    private final String dbPath;
    private Connection dbConnection;

    /**
     * Creates a new TableResultService with default configuration.
     */
    public TableResultService() {
        this.snippetConfig = SnippetConfig.DEFAULT;
        this.dbPath = DatabaseConfig.DEFAULT_DB_PATH;
    }

    /**
     * Creates a new TableResultService with custom snippet configuration.
     *
     * @param snippetConfig The snippet configuration to use
     */
    public TableResultService(SnippetConfig snippetConfig) {
        this.snippetConfig = snippetConfig;
        this.dbPath = DatabaseConfig.DEFAULT_DB_PATH;
    }

    /**
     * Creates a new TableResultService with custom database path.
     *
     * @param dbPath The path to the database file
     */
    public TableResultService(String dbPath) {
        this.snippetConfig = SnippetConfig.DEFAULT;
        this.dbPath = dbPath;
    }

    /**
     * Creates a new TableResultService with custom snippet configuration and database path.
     *
     * @param snippetConfig The snippet configuration to use
     * @param dbPath The path to the database file
     */
    public TableResultService(SnippetConfig snippetConfig, String dbPath) {
        this.snippetConfig = snippetConfig;
        this.dbPath = dbPath;
    }

    /**
     * Converts query results to a Tablesaw Table.
     *
     * @param query The original query
     * @param matches Set of matching document/sentence matches
     * @param variableBindings Variable bindings from query execution
     * @param indexes Map of indexes to retrieve additional document information
     * @return A Tablesaw Table containing the query results
     * @throws ResultGenerationException if an error occurs
     */
    public Table generateTable(
            Query query,
            Set<DocSentenceMatch> matches,
            VariableBindings variableBindings,
            Map<String, IndexAccess> indexes
    ) throws ResultGenerationException {
        Query.Granularity granularity = query.granularity();
        logger.debug("Generating table for {} matching {} at {} granularity",
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
                        return generateCountTable(countNode, matches, variableBindings);
                    }
                }
            }

            // Create column specifications (reusing from existing code)
            List<ColumnSpec> columnSpecs = createColumnSpecs(query, variableBindings, matches);
            
            // Create Tablesaw columns based on column specifications
            List<Column<?>> columns = createTablesawColumns(columnSpecs);
            
            // Create the Tablesaw table
            Table table = Table.create("QueryResults");
            for (Column<?> column : columns) {
                table.addColumns(column);
            }
            
            // Populate the table with data
            populateTable(table, query, matches, variableBindings, indexes, columnSpecs);
            
            // Apply ordering if specified
            if (!query.orderBy().isEmpty()) {
                logger.debug("Ordering results by {} criteria", query.orderBy().size());
                table = applyOrdering(table, query.orderBy());
            }
            
            // Apply limit if specified
            if (query.limit().isPresent()) {
                int limit = query.limit().get();
                logger.debug("Limiting results to {} rows", limit);
                if (limit < table.rowCount()) {
                    table = table.first(limit);
                }
            }
            
            logger.debug("Generated table with {} columns and {} rows", 
                    table.columnCount(), table.rowCount());
            
            return table;
        } catch (Exception e) {
            throw new ResultGenerationException(
                    "Failed to generate table: " + e.getMessage(),
                    e,
                    "table_result_service",
                    ResultGenerationException.ErrorType.INTERNAL_ERROR
            );
        } finally {
            closeDbConnection();
        }
    }

    /**
     * Creates Tablesaw columns based on column specifications.
     *
     * @param columnSpecs The column specifications
     * @return List of Tablesaw columns
     */
    private List<Column<?>> createTablesawColumns(List<ColumnSpec> columnSpecs) {
        List<Column<?>> columns = new ArrayList<>();
        
        for (ColumnSpec spec : columnSpecs) {
            String name = spec.name();
            ColumnType type = spec.type();
            
            // Create appropriate column type based on the data type
            if (type == ColumnType.DATE) {
                columns.add(DateColumn.create(name));
            } else if (type == ColumnType.COUNT) {
                columns.add(IntColumn.create(name));
            } else {
                // Default to string column for all other types
                columns.add(StringColumn.create(name));
            }
        }
        
        return columns;
    }

    /**
     * Populates a Tablesaw table with data from query results.
     *
     * @param table The table to populate
     * @param query The original query
     * @param matches Set of matching document/sentence matches
     * @param variableBindings Variable bindings from query execution
     * @param indexes Map of indexes to retrieve additional document information
     * @param columnSpecs The column specifications
     * @throws ResultGenerationException if an error occurs
     */
    private void populateTable(
            Table table,
            Query query,
            Set<DocSentenceMatch> matches,
            VariableBindings variableBindings,
            Map<String, IndexAccess> indexes,
            List<ColumnSpec> columnSpecs
    ) throws ResultGenerationException {
        // For each match, create a row in the table
        for (DocSentenceMatch match : matches) {
            // Add a new row to the table
            int rowIndex = table.rowCount();
            
            // Initialize all columns with default values first
            for (ColumnSpec columnSpec : columnSpecs) {
                String columnName = columnSpec.name();
                ColumnType columnType = columnSpec.type();
                
                // Set default values based on column type
                if (columnType == ColumnType.DATE) {
                    DateColumn column = (DateColumn) table.column(columnName);
                    column.appendMissing();
                } else if (columnType == ColumnType.COUNT) {
                    IntColumn column = (IntColumn) table.column(columnName);
                    column.appendMissing();
                } else {
                    StringColumn column = (StringColumn) table.column(columnName);
                    column.append((String)null);
                }
            }
            
            // Now set actual values for each column
            for (ColumnSpec columnSpec : columnSpecs) {
                String columnName = columnSpec.name();
                ColumnType columnType = columnSpec.type();
                
                // Get the value for this column
                String value = getValueForColumn(columnName, columnType, match, variableBindings, indexes, query);
                
                // Set the value in the appropriate column based on its type
                if (value != null) {
                    setColumnValue(table, rowIndex, columnName, columnType, value);
                }
            }
        }
    }

    /**
     * Sets a value in a Tablesaw column based on the column type.
     *
     * @param table The table
     * @param rowIndex The row index
     * @param columnName The column name
     * @param columnType The column type
     * @param value The string value to set
     */
    private void setColumnValue(Table table, int rowIndex, String columnName, ColumnType columnType, String value) {
        if (columnType == ColumnType.DATE) {
            try {
                DateColumn column = (DateColumn) table.column(columnName);
                LocalDate date = LocalDate.parse(value, DateTimeFormatter.ISO_DATE);
                column.set(rowIndex, date);
            } catch (DateTimeParseException e) {
                // If parsing fails, set a missing value
                DateColumn column = (DateColumn) table.column(columnName);
                column.setMissing(rowIndex);
            }
        } else if (columnType == ColumnType.COUNT) {
            try {
                IntColumn column = (IntColumn) table.column(columnName);
                column.set(rowIndex, Integer.parseInt(value));
            } catch (NumberFormatException e) {
                // If parsing fails, set a missing value
                IntColumn column = (IntColumn) table.column(columnName);
                column.setMissing(rowIndex);
            }
        } else {
            // Default to string column for all other types
            StringColumn column = (StringColumn) table.column(columnName);
            column.set(rowIndex, value);
        }
    }

    /**
     * Gets the value for a column from the appropriate source.
     * This is a simplified implementation - in a real implementation,
     * you would need to handle all the different column types and data sources.
     *
     * @param columnName The column name
     * @param columnType The column type
     * @param match The document/sentence match
     * @param variableBindings The variable bindings
     * @param indexes The indexes
     * @param query The original query
     * @return The value for the column
     */
    private String getValueForColumn(
            String columnName,
            ColumnType columnType,
            DocSentenceMatch match,
            VariableBindings variableBindings,
            Map<String, IndexAccess> indexes,
            Query query
    ) {
        // For document_id column, get the document ID
        if (columnName.equals("document_id")) {
            return String.valueOf(match.documentId());
        }
        
        // For sentence_id column, get the sentence ID
        if (columnName.equals("sentence_id")) {
            return String.valueOf(match.sentenceId());
        }
        
        // For metadata columns (title, timestamp, etc.)
        if (columnName.equals("title") || columnName.equals("timestamp") || 
            columnType == ColumnType.DATE) {
            try {
                return getMetadataValue(match.documentId(), columnName);
            } catch (SQLException e) {
                logger.error("Error retrieving metadata for document {}: {}", 
                    match.documentId(), e.getMessage());
                return null;
            }
        }
        
        // For variable columns, get the value from the variable bindings
        // Remove ? prefix if present
        String variableName = columnName;
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        // Get the first value for this variable
        Optional<String> valueOpt = variableBindings.getValueWithFallback(
            match.documentId(), match.sentenceId(), variableName);
        return valueOpt.orElse(null);
    }
    
    /**
     * Retrieves a metadata value from the database.
     *
     * @param documentId The document ID
     * @param fieldName The metadata field name
     * @return The metadata value, or null if not found
     * @throws SQLException if a database error occurs
     */
    private String getMetadataValue(int documentId, String fieldName) throws SQLException {
        Connection conn = getDbConnection();
        String sql = "SELECT " + fieldName + " FROM documents WHERE document_id = ?";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, documentId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(fieldName);
                }
            }
        }
        
        return null;
    }

    /**
     * Applies ordering to a Tablesaw table based on order specifications.
     *
     * @param table The table to order
     * @param orderSpecs The order specifications
     * @return The ordered table
     */
    private Table applyOrdering(Table table, List<OrderSpec> orderSpecs) {
        // For simplicity, we'll just return the table as is
        // In a real implementation, you would need to implement proper sorting
        logger.warn("Sorting not fully implemented - returning unsorted table");
        return table;
    }

    /**
     * Generates a count table for a COUNT query.
     *
     * @param countNode The COUNT node
     * @param matches The matches
     * @param variableBindings The variable bindings
     * @return A Tablesaw table with the count result
     */
    private Table generateCountTable(
            CountNode countNode,
            Set<DocSentenceMatch> matches,
            VariableBindings variableBindings
    ) {
        // Create a table with count and document_id columns
        IntColumn countColumn = IntColumn.create("count");
        StringColumn docIdColumn = StringColumn.create("document_id");
        Table table = Table.create("CountResult", countColumn, docIdColumn);
        
        // Calculate the count - just count all matches for simplicity
        int count = matches.size();
        
        // Add the count to the table
        if (count > 0) {
            countColumn.append(count);
            docIdColumn.append("all");
        } else {
            // Add a row with count 0 to ensure the table is not empty
            countColumn.append(0);
            docIdColumn.append("all");
        }
        
        return table;
    }

    /**
     * Creates column specifications based on the query and available variable bindings.
     * This method is similar to the one in ResultGenerator.
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
        Set<String> columnNames = new HashSet<>(); // Track column names to avoid duplicates
        
        // Add columns from SELECT clause first
        List<SelectColumn> selectColumns = query.selectColumns();
        if (selectColumns != null && !selectColumns.isEmpty()) {
            // Process each column in the SELECT clause
            for (SelectColumn column : selectColumns) {
                if (column instanceof CountNode || column instanceof CountColumn) {
                    addColumnIfNotExists(columns, columnNames, new ColumnSpec("count", ColumnType.COUNT));
                } else if (column instanceof MetadataColumn metadataColumn) {
                    // Handle metadata columns
                    if (metadataColumn.selectsAllFields()) {
                        // Add all metadata fields without the metadata_ prefix
                        addColumnIfNotExists(columns, columnNames, new ColumnSpec("title", ColumnType.TERM));
                        addColumnIfNotExists(columns, columnNames, new ColumnSpec("timestamp", ColumnType.TERM));
                    } else if (metadataColumn.getFieldName() != null) {
                        // Add specific metadata field without the metadata_ prefix
                        addColumnIfNotExists(columns, columnNames, new ColumnSpec(metadataColumn.getFieldName(), ColumnType.TERM));
                    }
                } else if (column instanceof SnippetColumn snippetColumn) {
                    // Handle snippet columns
                    String variableName = snippetColumn.getSnippetNode().variable();
                    if (variableName.startsWith("?")) {
                        variableName = variableName.substring(1);
                    }
                    addColumnIfNotExists(columns, columnNames, new ColumnSpec("snippet_" + variableName, ColumnType.SNIPPET));
                } else {
                    // Regular variable column
                    String columnName = column.toString();
                    // Remove the ? prefix if present
                    if (columnName.startsWith("?")) {
                        columnName = columnName.substring(1);
                    }
                    ColumnType columnType = inferColumnType(columnName, variableBindings);
                    addColumnIfNotExists(columns, columnNames, new ColumnSpec(columnName, columnType));
                }
            }
        } else {
            // If no SELECT columns specified, include all variables
            Set<String> variableNames = new HashSet<>();
            
            // Collect all variable names from matches
            for (DocSentenceMatch match : matches) {
                if (match.isSentenceLevel()) {
                    // Get variables for this sentence
                    Map<String, List<String>> sentVars = variableBindings.getValuesForSentence(match.documentId(), match.sentenceId());
                    variableNames.addAll(sentVars.keySet());
                } else {
                    // Get variables for this document
                    Map<String, List<String>> docVars = variableBindings.getValuesForDocument(match.documentId());
                    variableNames.addAll(docVars.keySet());
                }
            }
            
            // Add columns for each variable
            for (String variableName : variableNames) {
                ColumnType columnType = inferColumnType(variableName, variableBindings);
                addColumnIfNotExists(columns, columnNames, new ColumnSpec(variableName, columnType));
            }
        }
        
        // Always include document_id if not already added
        addColumnIfNotExists(columns, columnNames, new ColumnSpec("document_id", ColumnType.TERM));
        
        // Include sentence_id for sentence granularity if not already added
        if (query.granularity() == Query.Granularity.SENTENCE) {
            addColumnIfNotExists(columns, columnNames, new ColumnSpec("sentence_id", ColumnType.TERM));
        }
        
        return columns;
    }
    
    /**
     * Adds a column to the list if a column with the same name doesn't already exist.
     *
     * @param columns The list of columns
     * @param columnNames The set of column names
     * @param columnSpec The column specification to add
     */
    private void addColumnIfNotExists(List<ColumnSpec> columns, Set<String> columnNames, ColumnSpec columnSpec) {
        if (!columnNames.contains(columnSpec.name())) {
            columns.add(columnSpec);
            columnNames.add(columnSpec.name());
        }
    }

    /**
     * Infers the column type based on the variable name and bindings.
     *
     * @param variableName The variable name
     * @param variableBindings The variable bindings
     * @return The inferred column type
     */
    private ColumnType inferColumnType(String variableName, VariableBindings variableBindings) {
        // This is a simplified implementation
        // In a real implementation, you would need to infer the type based on
        // the variable bindings and other information
        
        // For now, just return a default type
        return ColumnType.TERM;
    }

    /**
     * Exports a Tablesaw table to a file in the specified format.
     *
     * @param table The table to export
     * @param format The export format (csv, json, html)
     * @param filename The filename to export to
     * @throws IOException if an error occurs during export
     */
    public void exportTable(Table table, String format, String filename) throws IOException {
        switch (format.toLowerCase()) {
            case "csv" -> {
                CsvWriteOptions options = CsvWriteOptions.builder(filename)
                        .header(true)
                        .build();
                table.write().csv(options);
            }
            case "json", "html" -> {
                // For simplicity, we'll just export as CSV for now
                // In a real implementation, you would need to add the appropriate
                // dependencies and implement proper JSON and HTML export
                CsvWriteOptions options = CsvWriteOptions.builder(filename)
                        .header(true)
                        .build();
                table.write().csv(options);
                logger.warn("Exporting as CSV instead of {} - full support requires additional dependencies", format);
            }
            default -> throw new IllegalArgumentException("Unsupported export format: " + format);
        }
    }

    /**
     * Gets a formatted string representation of a Tablesaw table.
     *
     * @param table The table to format
     * @return A string representation of the table
     */
    public String formatTable(Table table) {
        int totalRows = table.rowCount();
        int displayedRows = Math.min(totalRows, 20); // Tablesaw typically shows ~20 rows by default
        
        StringBuilder sb = new StringBuilder();
        sb.append(table.print());
        
        // Add a note about the preview if there are more rows than displayed
        if (totalRows > displayedRows) {
            sb.append("\n\nNote: This is a preview showing ").append(displayedRows)
              .append(" of ").append(totalRows).append(" total rows. Use export options to view all data.");
            sb.append("\nTo export all results, use: --export=csv:results.csv");
        }
        
        return sb.toString();
    }

    /**
     * Closes the database connection if it's open.
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

    /**
     * Gets or creates a database connection.
     *
     * @return The database connection
     * @throws SQLException if an error occurs
     */
    private Connection getDbConnection() throws SQLException {
        if (dbConnection == null || dbConnection.isClosed()) {
            String jdbcUrl = "jdbc:sqlite:" + dbPath;
            dbConnection = DriverManager.getConnection(jdbcUrl);
        }
        return dbConnection;
    }
} 
package com.example.query.result;

import com.example.query.executor.VariableBindings;
import com.example.query.model.CountNode;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.SelectColumn;
import com.example.core.IndexAccess;
import com.example.query.snippet.DatabaseConfig;
import com.example.query.snippet.SnippetConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvWriteOptions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
            // Create columns directly from SelectColumn objects
            List<Column<?>> columns = new ArrayList<>();
            
            // Add columns from SELECT clause
            List<SelectColumn> selectColumns = query.selectColumns();
            if (selectColumns != null && !selectColumns.isEmpty()) {
                for (SelectColumn selectColumn : selectColumns) {
                    columns.add(selectColumn.createColumn());
                }
            } else {
                // If no SELECT columns specified, we'll handle this later
                // after we've seen the matches
            }
            
            // Always include document_id
            StringColumn docIdColumn = StringColumn.create("document_id");
            columns.add(docIdColumn);
            
            // Include sentence_id for sentence granularity
            StringColumn sentIdColumn = null;
            if (granularity == Query.Granularity.SENTENCE) {
                sentIdColumn = StringColumn.create("sentence_id");
                columns.add(sentIdColumn);
            }
            
            // Create the table
            Table table = Table.create("QueryResults");
            for (Column<?> column : columns) {
                table.addColumns(column);
            }
            
            // Validate order by columns
            for (String orderColumn : query.orderBy()) {
                String columnName = orderColumn.startsWith("-") ? orderColumn.substring(1) : orderColumn;
                if (!table.columnNames().contains(columnName)) {
                    throw new ResultGenerationException(
                        String.format("Cannot order by column '%s'", columnName),
                        "table_result_service",
                        ResultGenerationException.ErrorType.INTERNAL_ERROR
                    );
                }
            }
            
            // Populate the table with data
            for (DocSentenceMatch match : matches) {
                // Add a new row
                int rowIndex = table.rowCount();
                table.appendRow();
                System.out.println("Added row " + rowIndex + " for match: " + match);
                
                // Set values for each column
                for (SelectColumn selectColumn : selectColumns) {
                    System.out.println("Populating column: " + selectColumn.getColumnName() + " for match: " + match);
                    selectColumn.populateColumn(table, rowIndex, match, variableBindings, indexes);
                    // After populating, check what the value is
                    Column<?> col = table.column(selectColumn.getColumnName());
                    System.out.println("Column " + selectColumn.getColumnName() + " now has value: " + col.get(rowIndex));
                }
                
                // Set document_id
                docIdColumn.set(rowIndex, String.valueOf(match.documentId()));
                
                // Set sentence_id if applicable
                if (sentIdColumn != null) {
                    sentIdColumn.set(rowIndex, String.valueOf(match.sentenceId()));
                }
            }
            
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
     * Applies ordering to a Tablesaw table based on order specifications.
     *
     * @param table The table to order
     * @param orderColumns The order columns (prefix with "-" for descending order)
     * @return The ordered table
     */
    private Table applyOrdering(Table table, List<String> orderColumns) {
        if (orderColumns.isEmpty()) {
            return table;
        }
        
        // Use Tablesaw's sortOn method with the column names
        logger.debug("Sorting table on columns: {}", orderColumns);
        return table.sortOn(orderColumns.toArray(new String[0]));
    }

    /**
     * Sorts a table by the given columns.
     * 
     * This method provides a direct way to sort tables using Tablesaw's column syntax.
     * Columns can be prefixed with "-" to indicate descending order.
     * 
     * @param table The table to sort
     * @param columns The columns to sort by
     * @return The sorted table
     */
    public Table sortTable(Table table, String... columns) {
        if (columns == null || columns.length == 0) {
            return table;
        }
        
        logger.debug("Sorting table on columns: {}", Arrays.toString(columns));
        return table.sortOn(columns);
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
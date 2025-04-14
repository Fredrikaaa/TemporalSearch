package com.example;

import com.example.query.*;
import com.example.query.executor.*;
import com.example.query.index.IndexManager;
import com.example.query.model.*;
import com.example.query.result.*;
import com.example.core.*;
import com.example.query.sqlite.SqliteAccessor;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Table;

import java.nio.file.Path;
import java.util.*;
import java.io.IOException;
import java.util.Map;

/**
 * Command-line interface for executing queries against the indexed corpus.
 * Serves as the entry point and orchestrator for the query engine.
 */
public class QueryCLI {
    private static final Logger logger = LoggerFactory.getLogger(QueryCLI.class);
    private final Path indexBaseDir;
    
    // Core components
    private final QueryParser parser;
    private final QuerySemanticValidator validator;
    private final QueryExecutor executor;

    /**
     * Creates a new QueryCLI instance.
     *
     * @param indexBaseDir The base directory for all index sets
     */
    public QueryCLI(Path indexBaseDir) {
        this.indexBaseDir = indexBaseDir;
        this.parser = new QueryParser();
        this.validator = new QuerySemanticValidator();
        this.executor = new QueryExecutor(new ConditionExecutorFactory());
        
        // Initialize the SqliteAccessor singleton
        SqliteAccessor.initialize(indexBaseDir.toString());
        
        logger.info("Initialized QueryCLI with base directory: {}", indexBaseDir);
        logger.info("Using database structure: {}/[CORPUS_NAME]/[CORPUS_NAME].db", indexBaseDir);
    }
    
    /**
     * Executes a query string.
     *
     * @param queryStr The query string to execute
     * @param exportFormat Optional export format (csv, json, html)
     * @param exportFilename Optional export filename
     */
    public void executeQuery(String queryStr, Optional<String> exportFormat, Optional<String> exportFilename) {
        try {
            // 1. Parse query string into Query object
            logger.debug("Parsing query: {}", queryStr);
            Query query = parser.parse(queryStr);
            
            // 2. Validate query semantics
            logger.debug("Validating query: {}", query);
            validator.validate(query);
            
            // Check for date queries and display helpful information
            checkAndDisplayDateQueryHelp(queryStr, query);
            
            // 3. Get index path from FROM clause
            String indexSetName = query.source();
            logger.debug("Using index set: {}", indexSetName);
            
            // Update database path to match the corpus name from FROM clause
            String corpusDbPath = Path.of(indexBaseDir.toString(), indexSetName, indexSetName + ".db").toString();
            logger.debug("Using database path based on corpus: {}", corpusDbPath);
            
            // Check if corpus-specific database exists
            if (!new java.io.File(corpusDbPath).exists()) {
                String errorMessage = String.format("Database file not found: %s. Each corpus must have a database in [index-dir]/%s/%s.db", 
                                                   corpusDbPath, indexSetName, indexSetName);
                logger.error(errorMessage);
                System.err.println("Error: " + errorMessage);
                System.err.println(String.format("Expected database location: %s/%s/%s.db", indexBaseDir, indexSetName, indexSetName));
                return; // Early return to avoid further processing
            }
            
            // Initialize Nash temporal index for this corpus
            logger.debug("Initializing Nash temporal index for corpus: {}", indexSetName);

            // Create a new TableResultService with the corpus-specific database path
            TableResultService tableResultService = new TableResultService(corpusDbPath);
            logger.info("Using corpus-specific database at: {}", corpusDbPath);
            
            // 4. Create IndexManager for the resolved path
            try (IndexManager indexManager = new IndexManager(indexBaseDir, indexSetName)) {
                logger.debug("Created IndexManager for index set: {}", indexSetName);
                
                // Initialize Nash index with the index manager
                executor.initializeNashIndex(indexSetName, indexManager);
                
                // 5. Execute query using QueryExecutor
                logger.debug("Executing query against index set: {}", indexSetName);
                Query.Granularity granularity = query.granularity();
                int windowSize = query.granularitySize().orElse(0); // Use 0 if not present
                logger.info("Query granularity: {} with size: {}", granularity, windowSize);
                
                // executor.execute now returns QueryResult
                QueryResult result = executor.execute(query, indexManager.getAllIndexes());
                
                // --- Adapt Match Count Display --- 
                // Use result.getAllDetails().size() as a proxy for count. 
                // TODO: Improve count logic based on QueryResult granularity/structure if needed.
                int matchCount = result.getAllDetails().size(); 
                String matchUnit = (granularity == Query.Granularity.DOCUMENT) ? "documents (approx details)" : "sentences (approx details)";
                logger.info("Query executed, found {} matching details (granularity: {})", matchCount, granularity);
                System.out.println("Total matches: " + matchCount + " " + matchUnit);
                // Removed specific document ID counting block

                // 6. Generate results using TableResultService 
                logger.debug("Generating result table");
                Table resultTable = tableResultService.generateTable(
                    query, 
                    result, // Pass QueryResult 
                    indexManager.getAllIndexes()
                );
                
                // NOTE: We're now using Tablesaw's sorting capabilities directly
                // The orderBy list in Query now contains Tablesaw-compatible sort strings
                // (column names with optional "-" prefix for descending order)
                
                // 7. Handle export if requested
                if (exportFormat.isPresent() && exportFilename.isPresent()) {
                    String format = exportFormat.get();
                    String filename = exportFilename.get();
                    logger.info("Exporting results to {} format in file: {}", format, filename);
                    
                    try {
                        tableResultService.exportTable(resultTable, format, filename);
                        System.out.println("Results exported to " + filename);
                    } catch (IOException e) {
                        logger.error("Error exporting results: {}", e.getMessage());
                        System.err.println("Error exporting results: " + e.getMessage());
                    }
                } else {
                    // 8. Format and display results
                    logger.debug("Formatting results for display");
                    String formattedResults = tableResultService.formatTable(resultTable);
                    
                    // Output the formatted results
                    System.out.println(formattedResults);
                }
            }
            
        } catch (QueryParseException e) {
            logger.error("Query parse error: {}", e.getMessage());
            System.err.println("Error parsing query: " + e.getMessage());
            // No position information in QueryParseException
        } catch (Exception e) {
            logger.error("Error executing query: {}", e.getMessage(), e);
            System.err.println("Error: " + e.getMessage());
        }
    }
    
    /**
     * Checks if the query involves date operations and displays helpful information.
     * 
     * @param queryStr The original query string
     * @param query The parsed query object
     */
    private void checkAndDisplayDateQueryHelp(String queryStr, Query query) {
        if (queryStr.toUpperCase().contains("DATE(")) {
            // Check for specific predicates
            if (queryStr.toUpperCase().contains("DATE(CONTAINS [")) {
                logger.info("Date CONTAINS query detected - requires dates to be fully within range");
                System.out.println("\nQuery Help: You're using DATE(CONTAINS [range]) which requires dates to be fully within the specified range.");
                System.out.println("For broader matches, consider using DATE(INTERSECT [range]) which matches any overlap with the range.\n");
            } else if (queryStr.toUpperCase().contains("DATE(INTERSECT [")) {
                logger.info("Date INTERSECT query detected - matches any overlap with date range");
            }
            
            // Check for granularity
            if (query.granularity() == Query.Granularity.SENTENCE) {
                logger.info("Date query with sentence granularity detected");
                System.out.println("Note: With sentence granularity, the query will return specific sentences containing date mentions in the specified range.");
            } else {
                logger.info("Date query with document granularity detected");
                System.out.println("Note: With document granularity, the query will return documents containing date mentions in the specified range.");
            }
        }
    }
    
    /**
     * Main entry point for the CLI.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        // Set up argument parser
        ArgumentParser parser = ArgumentParsers.newFor("QueryCLI").build()
                .defaultHelp(true)
                .description("Execute queries against indexed corpus. Supports extracting text snippets with SNIPPET(variableName) in SELECT clause.");
        
        parser.addArgument("-d", "--index-dir")
                .setDefault("indexes")
                .help("Base directory for index sets");
        
        parser.addArgument("--export")
                .help("Export results to a file in the specified format: csv:filename.csv, json:filename.json, or html:filename.html");
        
        parser.addArgument("query")
                .nargs("?")
                .help("Query string to execute");
        
        try {
            // Parse arguments
            Namespace ns = parser.parseArgs(args);
            String indexDir = ns.getString("index_dir");
            String query = ns.getString("query");
            String exportArg = ns.getString("export");
            
            // Parse export argument if provided
            Optional<String> exportFormat = Optional.empty();
            Optional<String> exportFilename = Optional.empty();
            
            if (exportArg != null && !exportArg.isEmpty()) {
                String[] parts = exportArg.split(":", 2);
                if (parts.length == 2) {
                    exportFormat = Optional.of(parts[0]);
                    exportFilename = Optional.of(parts[1]);
                } else {
                    System.err.println("Invalid export format. Use format:filename (e.g., csv:results.csv)");
                    System.exit(1);
                }
            }
            
            // Create and run CLI
            QueryCLI cli = new QueryCLI(Path.of(indexDir));
            
            if (query != null) {
                // Execute the provided query
                cli.executeQuery(query, exportFormat, exportFilename);
            } else {
                // Interactive mode
                Scanner scanner = new Scanner(System.in);
                System.out.println("Query CLI - Enter queries or 'exit' to quit");
                System.out.println("Using index directory: " + indexDir);
                System.out.println("Database structure: " + indexDir + "/[CORPUS_NAME]/[CORPUS_NAME].db");
                System.out.println("Snippet support is enabled. Use SNIPPET(variable) in SELECT clause to show text context.");
                System.out.println("Export support: Add --export=format:filename to export results (formats: csv, json, html)");
                
                while (true) {
                    System.out.print("\nQuery> ");
                    String input = scanner.nextLine().trim();
                    
                    if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                        break;
                    }
                    
                    if (!input.isEmpty()) {
                        cli.executeQuery(input, exportFormat, exportFilename);
                    }
                }
                
                scanner.close();
            }
            
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }
} 
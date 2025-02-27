package com.example;

import com.example.query.*;
import com.example.query.executor.*;
import com.example.query.index.IndexManager;
import com.example.query.model.*;
import com.example.query.model.column.ColumnSpec;
import com.example.query.model.column.ColumnType;
import com.example.query.result.*;
import com.example.query.snippet.DatabaseConfig;
import com.example.query.snippet.SnippetConfig;
import com.example.query.snippet.DatabaseDiagnostic;
import com.example.core.*;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.io.IOException;

/**
 * Command-line interface for executing queries against the indexed corpus.
 * Serves as the entry point and orchestrator for the query engine.
 */
public class QueryCLI {
    private static final Logger logger = LoggerFactory.getLogger(QueryCLI.class);
    private final Path indexBaseDir;
    private final String dbPath;

    // Core components
    private final QueryParser parser;
    private final QuerySemanticValidator validator;
    private final QueryExecutor executor;
    private final ResultGenerator resultGenerator;
    private final ResultFormatter resultFormatter;
    private final SnippetConfig snippetConfig;

    /**
     * Creates a new QueryCLI instance.
     *
     * @param indexBaseDir The base directory for all index sets
     */
    public QueryCLI(Path indexBaseDir) {
        this(indexBaseDir, DatabaseConfig.DEFAULT_DB_PATH);
    }
    
    /**
     * Creates a new QueryCLI instance with a custom database path.
     *
     * @param indexBaseDir The base directory for all index sets
     * @param dbPath The path to the database file
     */
    public QueryCLI(Path indexBaseDir, String dbPath) {
        this.indexBaseDir = indexBaseDir;
        this.dbPath = dbPath;
        this.parser = new QueryParser();
        this.validator = new QuerySemanticValidator();
        this.executor = new QueryExecutor(new ConditionExecutorFactory());
        this.snippetConfig = SnippetConfig.DEFAULT;
        this.resultGenerator = new ResultGenerator(snippetConfig, dbPath);
        this.resultFormatter = new ResultFormatter();
        
        logger.info("Initialized QueryCLI with base directory: {} and database: {}", indexBaseDir, dbPath);
    }

    /**
     * Executes a query string.
     *
     * @param queryStr The query string to execute
     */
    public void executeQuery(String queryStr) {
        try {
            // 1. Parse query string into Query object
            logger.debug("Parsing query: {}", queryStr);
            Query query = parser.parse(queryStr);
            
            // 2. Validate query semantics
            logger.debug("Validating query: {}", query);
            validator.validate(query);
            
            // 3. Get index path from FROM clause
            String indexSetName = query.getSource();
            logger.debug("Using index set: {}", indexSetName);
            
            // 4. Create IndexManager for the resolved path
            try (IndexManager indexManager = new IndexManager(indexBaseDir, indexSetName)) {
                logger.debug("Created IndexManager for index set: {}", indexSetName);
                
                // 5. Execute query using QueryExecutor
                logger.debug("Executing query against index set: {}", indexSetName);
                Query.Granularity granularity = query.getGranularity().orElse(Query.Granularity.DOCUMENT);
                Set<DocSentenceMatch> matches = executor.execute(query, indexManager.getAllIndexes());
                VariableBindings variableBindings = executor.getVariableBindings();
                
                // Display the total number of matches based on granularity
                if (granularity == Query.Granularity.DOCUMENT) {
                    Set<Integer> documentIds = executor.getDocumentIds(matches);
                    logger.info("Query executed, found {} matching documents", documentIds.size());
                    System.out.println("Total matches: " + documentIds.size() + " documents");
                } else {
                    logger.info("Query executed, found {} matching sentences", matches.size());
                    System.out.println("Total matches: " + matches.size() + " sentences");
                }
                
                // 6. Generate results using ResultGenerator
                logger.debug("Generating result table");
                ResultTable resultTable = resultGenerator.generateResultTable(
                    query, matches, variableBindings, indexManager.getAllIndexes());
                
                // 7. Format and display results
                logger.debug("Formatting results");
                String formattedResults = resultFormatter.format(resultTable);
                
                // Output the formatted results
                System.out.println(formattedResults);
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
        
        parser.addArgument("--db")
                .setDefault(DatabaseConfig.DEFAULT_DB_PATH)
                .help("Path to the SQLite database file (required for snippet functionality)");
        
        parser.addArgument("--check-db")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Run diagnostics on the database to check snippet functionality");
        
        parser.addArgument("query")
                .nargs("?")
                .help("Query string to execute");
        
        try {
            // Parse arguments
            Namespace ns = parser.parseArgs(args);
            String indexDir = ns.getString("index_dir");
            String dbPath = ns.getString("db");
            String query = ns.getString("query");
            boolean checkDb = ns.getBoolean("check_db");
            
            // Run database diagnostics if requested
            if (checkDb) {
                System.out.println("Running database diagnostics...");
                DatabaseDiagnostic.checkDatabase(dbPath);
                // If no query was provided, exit after diagnostics
                if (query == null) {
                    return;
                }
            }
            
            // Create and run CLI
            QueryCLI cli = new QueryCLI(Path.of(indexDir), dbPath);
            
            if (query != null) {
                // Execute the provided query
                cli.executeQuery(query);
            } else {
                // Interactive mode
                Scanner scanner = new Scanner(System.in);
                System.out.println("Query CLI - Enter queries or 'exit' to quit");
                System.out.println("Using index directory: " + indexDir);
                System.out.println("Using database: " + dbPath);
                System.out.println("Snippet support is enabled. Use SNIPPET(variable) in SELECT clause to show text context.");
                System.out.println("For database diagnostics, enter: .check-db");
                
                while (true) {
                    System.out.print("\nQuery> ");
                    String input = scanner.nextLine().trim();
                    
                    if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                        break;
                    }
                    
                    if (input.equalsIgnoreCase(".check-db")) {
                        System.out.println("Running database diagnostics...");
                        DatabaseDiagnostic.checkDatabase(dbPath);
                        continue;
                    }
                    
                    if (!input.isEmpty()) {
                        cli.executeQuery(input);
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
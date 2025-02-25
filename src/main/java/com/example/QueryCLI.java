package com.example;

import com.example.query.*;
import com.example.query.model.*;
import com.example.core.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.io.IOException;

/**
 * Command-line interface for executing queries against the indexed corpus.
 * Supports all query types defined in the query language grammar.
 */
public class QueryCLI {
    private static final Logger logger = LoggerFactory.getLogger(QueryCLI.class);
    private final QueryParser parser;
    private final QuerySemanticValidator validator;
    private final Map<String, IndexAccess> indexes;
    private final Path indexBaseDir;

    public QueryCLI(Path indexBaseDir) {
        this.indexBaseDir = indexBaseDir;
        this.parser = new QueryParser();
        this.validator = new QuerySemanticValidator();
        this.indexes = new HashMap<>();
    }

    private void initializeIndexes() throws IndexAccessException {
        Options options = new Options();
        options.createIfMissing(false);  // Don't create if missing
        options.cacheSize(64 * 1024 * 1024);  // 64MB cache

        // Initialize all required indexes
        String[] indexTypes = {
            "unigram", "bigram", "trigram", "pos", "ner_date", 
            "dependency", "hypernym"
        };

        boolean hasUnigram = false;  // Track if we have the essential unigram index

        for (String type : indexTypes) {
            try {
                Path indexPath = indexBaseDir.resolve(type);
                if (!java.nio.file.Files.exists(indexPath) || 
                    !java.nio.file.Files.exists(indexPath.resolve("CURRENT"))) {
                    if (!java.nio.file.Files.exists(indexPath)) {
                        logger.warn("Index directory {} does not exist", indexPath);
                    } else {
                        logger.error("Index directory {} exists but does not contain valid LevelDB files", indexPath);
                    }
                    continue;
                }

                boolean hasManifest = false;
                try {
                    hasManifest = java.nio.file.Files.list(indexPath)
                        .anyMatch(p -> p.getFileName().toString().startsWith("MANIFEST-"));
                } catch (IOException e) {
                    logger.error("Failed to check for manifest file in {}: {}", indexPath, e.getMessage());
                }

                if (!hasManifest) {
                    logger.error("Index directory {} exists but does not contain valid LevelDB files", indexPath);
                    continue;
                }

                indexes.put(type, new IndexAccess(indexPath, type, options));
                logger.info("Initialized {} index", type);
                
                if (type.equals("unigram")) {
                    hasUnigram = true;
                }
            } catch (IndexAccessException e) {
                logger.error("Failed to initialize {} index: {}", type, e.getMessage());
                // Continue with other indexes
            }
        }

        if (indexes.isEmpty()) {
            throw new IndexAccessException(
                "No indexes could be initialized. Please ensure the index directory contains valid LevelDB databases.",
                "all",
                IndexAccessException.ErrorType.INITIALIZATION_ERROR
            );
        }

        if (!hasUnigram) {
            throw new IndexAccessException(
                "Unigram index is required but could not be initialized. Please ensure the unigram index exists and is valid.",
                "unigram",
                IndexAccessException.ErrorType.INITIALIZATION_ERROR
            );
        }
    }

    private void executeQuery(String queryStr) {
        try {
            // Parse and validate query
            Query query = parser.parse(queryStr);
            validator.validate(query);
            logger.debug("Parsed and validated query: {}", query);

            // Check if this is a COUNT query
            boolean isCountQuery = false;
            CountNode countNode = null;
            
            // TODO: Extract the count node from the select list when it's implemented
            // For now, we'll just check if the query has a COUNT in it
            if (queryStr.toUpperCase().contains("COUNT(")) {
                isCountQuery = true;
                // For demonstration, we'll assume COUNT(*) for now
                countNode = CountNode.countAll();
                logger.debug("Detected COUNT query: {}", countNode);
            }

            // Execute query conditions
            Set<Integer> results = new HashSet<>();
            boolean firstCondition = true;

            for (Condition condition : query.getConditions()) {
                Set<Integer> conditionResults = executeCondition(condition);
                
                if (firstCondition) {
                    results.addAll(conditionResults);
                    firstCondition = false;
                } else {
                    // Implement AND semantics
                    results.retainAll(conditionResults);
                }
            }

            // Create a result processor for applying result control operations
            ResultProcessor resultProcessor = new ResultProcessor();
            
            // Convert document IDs to result rows
            List<Map<String, String>> resultRows = new ArrayList<>();
            for (Integer docId : results) {
                Map<String, String> row = new HashMap<>();
                row.put("document_id", docId.toString());
                // TODO: Fetch additional document details
                resultRows.add(row);
            }
            
            // Create a result table
            List<com.example.query.model.column.ColumnSpec> columns = new ArrayList<>();
            columns.add(new com.example.query.model.column.ColumnSpec(
                "document_id", 
                com.example.query.model.column.ColumnType.TERM
            ));
            // TODO: Add more columns based on query select list
            
            ResultTable resultTable = new ResultTable(
                columns, 
                resultRows, 
                10, 
                com.example.query.format.TableConfig.getDefault()
            );
            
            // Apply ordering if specified
            if (!query.getOrderBy().isEmpty()) {
                logger.debug("Applying ordering: {}", query.getOrderBy());
                resultTable = resultProcessor.applyOrdering(resultTable, query.getOrderBy());
            }

            // Apply limit if specified
            if (query.getLimit().isPresent()) {
                int limit = query.getLimit().get();
                logger.debug("Applying limit: {}", limit);
                resultTable = resultProcessor.applyLimit(resultTable, limit);
            }

            // Handle COUNT queries
            if (isCountQuery && countNode != null) {
                int count;
                String columnName = null;
                
                switch (countNode.type()) {
                    case ALL:
                        count = resultProcessor.countAll(resultTable);
                        break;
                    case UNIQUE:
                        columnName = countNode.variable().orElse("document_id");
                        count = resultProcessor.countUnique(resultTable, columnName);
                        break;
                    case DOCUMENTS:
                        count = resultProcessor.countDocuments(resultTable);
                        break;
                    default:
                        throw new IllegalStateException("Unknown count type: " + countNode.type());
                }
                
                // Create a count result table
                resultTable = resultProcessor.createCountResultTable(
                    count, 
                    countNode.type(), 
                    columnName
                );
                
                // Display count result
                System.out.printf("Count result: %d%n", count);
            } else {
                // Display regular results
                System.out.printf("Found %d matching documents%n", resultTable.getRowCount());
                
                // Format and display the result table
                for (int i = 0; i < resultTable.getRowCount(); i++) {
                    System.out.printf("Document ID: %s%n", resultTable.getValue(i, "document_id"));
                    // TODO: Display more document details
                }
            }

        } catch (QueryParseException e) {
            System.err.println("Error parsing query: " + e.getMessage());
            // Don't log stack trace for parse errors as they are expected user errors
        } catch (Exception e) {
            // For unexpected errors, still log the full stack trace
            System.err.println("Error executing query: " + e.getMessage());
            logger.error("Query execution failed", e);
        }
    }

    private Set<Integer> executeCondition(Condition condition) throws IndexAccessException {
        Set<Integer> results = new HashSet<>();

        if (condition instanceof ContainsCondition) {
            ContainsCondition contains = (ContainsCondition) condition;
            IndexAccess unigramIndex = indexes.get("unigram");
            if (unigramIndex == null) {
                throw new IndexAccessException(
                    "Unigram index not initialized but required for CONTAINS condition",
                    "unigram",
                    IndexAccessException.ErrorType.INITIALIZATION_ERROR
                );
            }
            // Search in unigram index
            Optional<PositionList> positions = unigramIndex.get(
                contains.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            positions.ifPresent(pos -> pos.getPositions().forEach(p -> results.add(p.getDocumentId())));
        } else if (condition instanceof NerCondition) {
            NerCondition ner = (NerCondition) condition;
            throw new UnsupportedOperationException(
                "NER queries are not yet implemented. The NER index needs to be built first. " +
                "Please use other query types like CONTAINS for now."
            );
        } else if (condition instanceof TemporalCondition) {
            TemporalCondition temporal = (TemporalCondition) condition;
            String key = temporal.getStartDate().format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
            
            switch (temporal.getTemporalType()) {
                case BEFORE:
                    // Scan all dates before the target date using forward iteration
                    try (DBIterator it = indexes.get("ner_date").iterator()) {
                        it.seekToFirst();  // Start from the beginning
                        while (it.hasNext()) {
                            Map.Entry<byte[], byte[]> entry = it.next();
                            String currentKey = new String(entry.getKey(), java.nio.charset.StandardCharsets.UTF_8);
                            if (currentKey.compareTo(key) >= 0) {
                                break;  // Stop when we reach or exceed the target date
                            }
                            PositionList positions = PositionList.deserialize(entry.getValue());
                            positions.getPositions().forEach(p -> results.add(p.getDocumentId()));
                        }
                    } catch (IOException e) {
                        throw new IndexAccessException(
                            "Error scanning temporal index: " + e.getMessage(),
                            "ner_date",
                            IndexAccessException.ErrorType.READ_ERROR,
                            e
                        );
                    }
                    break;
                    
                case AFTER:
                    // Scan all dates after the target date
                    try (DBIterator it = indexes.get("ner_date").iterator()) {
                        for (it.seek(key.getBytes(java.nio.charset.StandardCharsets.UTF_8)); it.hasNext(); it.next()) {
                            PositionList positions = PositionList.deserialize(it.peekNext().getValue());
                            positions.getPositions().forEach(p -> results.add(p.getDocumentId()));
                        }
                    } catch (IOException e) {
                        throw new IndexAccessException(
                            "Error scanning temporal index: " + e.getMessage(),
                            "ner_date",
                            IndexAccessException.ErrorType.READ_ERROR,
                            e
                        );
                    }
                    break;
                    
                case BETWEEN:
                    String endKey = temporal.getEndDate().get()
                        .format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
                    // Scan all dates between start and end
                    try (DBIterator it = indexes.get("ner_date").iterator()) {
                        for (it.seek(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                             it.hasNext() && new String(it.peekNext().getKey()).compareTo(endKey) <= 0;
                             it.next()) {
                            PositionList positions = PositionList.deserialize(it.peekNext().getValue());
                            positions.getPositions().forEach(p -> results.add(p.getDocumentId()));
                        }
                    } catch (IOException e) {
                        throw new IndexAccessException(
                            "Error scanning temporal index: " + e.getMessage(),
                            "ner_date",
                            IndexAccessException.ErrorType.READ_ERROR,
                            e
                        );
                    }
                    break;
                    
                case NEAR:
                    // TODO: Implement NEAR with range
                    logger.warn("NEAR temporal queries not yet implemented");
                    break;
            }
        } else if (condition instanceof DependencyCondition) {
            DependencyCondition dep = (DependencyCondition) condition;
            // Create key as GOVERNOR:RELATION:DEPENDENT
            String key = String.join(":", dep.getGovernor(), dep.getRelation(), dep.getDependent());
            Optional<PositionList> positions = indexes.get("dependency").get(
                key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            positions.ifPresent(pos -> pos.getPositions().forEach(p -> results.add(p.getDocumentId())));
        }

        return results;
    }

    private void close() {
        for (IndexAccess index : indexes.values()) {
            try {
                index.close();
            } catch (IndexAccessException e) {
                logger.warn("Error closing index: {}", e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("QueryCLI").build()
            .defaultHelp(true)
            .description("Execute queries against the indexed corpus");

        parser.addArgument("-i", "--index-dir")
            .setDefault("indexes")
            .help("Base directory containing the indexes (default: indexes)");

        parser.addArgument("-q", "--query")
            .help("Query to execute (if not provided, will read from stdin)");

        try {
            Namespace ns = parser.parseArgs(args);
            Path indexDir = Path.of(ns.getString("index_dir"));
            String query = ns.getString("query");

            QueryCLI cli = new QueryCLI(indexDir);

            try {
                cli.initializeIndexes();

                if (query != null) {
                    // Execute single query
                    cli.executeQuery(query);
                } else {
                    // Interactive mode
                    Scanner scanner = new Scanner(System.in);
                    System.out.println("Enter queries (empty line to exit):");
                    
                    while (true) {
                        System.out.print("> ");
                        String line = scanner.nextLine().trim();
                        if (line.isEmpty()) {
                            break;
                        }
                        cli.executeQuery(line);
                    }
                }
            } finally {
                cli.close();
            }

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("Application error", e);
            System.exit(1);
        }
    }
} 
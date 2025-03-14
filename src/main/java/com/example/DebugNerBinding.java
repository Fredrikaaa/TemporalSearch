package com.example;

import com.example.core.IndexAccess;
import com.example.query.executor.ConditionExecutorFactory;
import com.example.query.executor.QueryExecutor;
import com.example.query.executor.VariableBindings;
import com.example.query.index.IndexManager;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.ResultTable;
import com.example.query.QueryParser;
import com.example.query.QueryParseException;
import com.example.query.result.ResultGenerator;

import java.util.Map;
import java.util.Set;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugNerBinding {
    private static final Logger logger = LoggerFactory.getLogger(DebugNerBinding.class);
    
    public static void main(String[] args) {
        try {
            // Replace with an actual path to your database
            String dbPath = "db.sqlite";
            
            // Create a query that uses NER with a variable
            String queryStr = "SELECT ?person FROM wikipedia WHERE CONTAINS(\"President\") AND NER(PERSON, ?person) GRANULARITY SENTENCE";
            logger.info("Executing query: {}", queryStr);
            
            // Parse the query
            QueryParser parser = new QueryParser();
            Query query = parser.parse(queryStr);
            logger.info("Parsed query: {}", query);
            
            // Load indexes
            Path indexBaseDir = Paths.get("indexes");
            String indexSetName = "wikipedia"; // Same as in the FROM clause
            IndexManager indexManager = new IndexManager(indexBaseDir, indexSetName);
            Map<String, IndexAccess> indexes = indexManager.getAllIndexes();
            
            // Execute the query
            QueryExecutor executor = new QueryExecutor(new ConditionExecutorFactory());
            Set<DocSentenceMatch> matches = executor.execute(query, indexes);
            VariableBindings bindings = executor.getVariableBindings();
            
            logger.info("Query matched {} documents/sentences", matches.size());
            logger.info("Variable bindings: {}", bindings);
            
            // Generate results
            ResultGenerator resultGenerator = new ResultGenerator(dbPath);
            ResultTable resultTable = resultGenerator.generateResultTable(query, matches, bindings, indexes);
            
            logger.info("Result table has {} columns and {} rows", 
                       resultTable.getColumns().size(), resultTable.getRows().size());
            
            // Print column names
            logger.info("Columns: {}", resultTable.getColumns().stream()
                      .map(col -> col.name())
                      .collect(java.util.stream.Collectors.joining(", ")));
            
            // Print first few rows
            int rowsToPrint = Math.min(5, resultTable.getRows().size());
            for (int i = 0; i < rowsToPrint; i++) {
                logger.info("Row {}: {}", i, resultTable.getRows().get(i));
            }
            
            // Close the index manager
            indexManager.close();
        } catch (Exception e) {
            logger.error("Error executing query", e);
        }
    }
} 
package com.example.query;

import com.example.query.executor.ConditionExecutorFactory;
import com.example.query.executor.QueryExecutionException;
import com.example.query.executor.QueryExecutor;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.query.index.IndexManager;
import com.example.query.sqlite.SqliteAccessor;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for query execution.
 * This test class runs a variety of query examples against the actual dataset
 * to verify they execute successfully.
 * 
 * These tests only verify that queries can run without errors, not the specific results.
 */
@Tag("e2e")
@DisplayName("Query End-to-End Tests")
public class QueryEndToEndTest {
    private static final Logger logger = LoggerFactory.getLogger(QueryEndToEndTest.class);
    
    // Path to index directory - update with your actual path
    private static final String INDEX_BASE_DIR = System.getProperty("index.dir", "indexes");
    
    // Test corpus/source name - update with your actual corpus name
    private static final String TEST_CORPUS = "wikipedia";
    
    private static IndexManager indexManager;
    private static Map<String, IndexAccess> indexes;
    
    private QueryParser parser;
    private QuerySemanticValidator validator;
    private QueryExecutor executor;
    
    @BeforeAll
    static void setUpIndexes() {
        // Initialize SqliteAccessor
        Path indexPath = Paths.get(INDEX_BASE_DIR);
        SqliteAccessor.initialize(indexPath.toString());
        
        try {
            // Initialize IndexManager with the corpus name
            indexManager = new IndexManager(indexPath, TEST_CORPUS);
            
            // Get all available indexes
            indexes = indexManager.getAllIndexes();
            
            // Verify indexes are loaded
            if (indexes.isEmpty()) {
                logger.warn("No indexes loaded for corpus: {}. Tests may fail.", TEST_CORPUS);
            } else {
                logger.info("Loaded {} indexes for corpus: {}", indexes.size(), TEST_CORPUS);
            }
        } catch (IndexAccessException e) {
            logger.error("Failed to load indexes for corpus: {}", TEST_CORPUS, e);
            fail("Failed to load indexes: " + e.getMessage());
        }
    }
    
    @BeforeEach
    void setUp() {
        parser = new QueryParser();
        validator = new QuerySemanticValidator();
        executor = new QueryExecutor(new ConditionExecutorFactory());
    }
    
    /**
     * Helper method to execute a query and verify it runs without errors.
     * 
     * @param queryStr The query string to execute
     */
    private void executeAndVerify(String queryStr) {
        try {
            // Parse the query
            Query query = parser.parse(queryStr);
            
            // Validate the query
            validator.validate(query);
            
            // Execute the query
            Set<DocSentenceMatch> results = executor.execute(query, indexes);
            
            // Verify execution completed successfully
            assertNotNull(results, "Query execution returned null results");
            
            // Log result count for informational purposes
            logger.info("Query '{}' returned {} results", queryStr, results.size());
            
        } catch (QueryParseException | QueryExecutionException e) {
            logger.error("Query execution failed for: {}", queryStr, e);
            fail("Query execution failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Simple query without conditions")
    void simpleQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS);
    }
    
    @Test
    @DisplayName("Basic text query with CONTAINS")
    void basicTextQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + " WHERE CONTAINS(\"artificial intelligence\")");
    }
    
    @Test
    @DisplayName("Query with named entity recognition")
    void nerQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + " WHERE NER(\"PERSON\")");
    }
    
    @Test
    @DisplayName("Query with entity binding")
    void entityBindingQuery() {
        executeAndVerify("SELECT ?person FROM " + TEST_CORPUS + " WHERE NER(\"PERSON\") AS ?person");
    }
    
    @Test
    @DisplayName("Query with date restriction")
    void dateQuery() {
        executeAndVerify("SELECT ?date FROM " + TEST_CORPUS + " WHERE DATE(< 2000) AS ?date");
    }
    
    @Test
    @DisplayName("Query with dependency relation")
    void dependencyQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + " WHERE DEPENDS(\"subject\", \"nsubj\", \"verb\")");
    }
    
    @Test
    @DisplayName("Query with combined conditions using AND")
    void combinedQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + 
                         " WHERE CONTAINS(\"science\") AND NER(\"PERSON\")");
    }
    
    @Test
    @DisplayName("Query with OR condition")
    void orConditionQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + 
                         " WHERE CONTAINS(\"physics\") OR CONTAINS(\"chemistry\")");
    }
    
    @Test
    @DisplayName("Query with NOT condition")
    void notConditionQuery() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + 
                         " WHERE CONTAINS(\"science\") AND NOT CONTAINS(\"fiction\")");
    }
    
    @Test
    @DisplayName("Query with ORDER BY and LIMIT")
    void queryWithOrderAndLimit() {
        executeAndVerify("SELECT COUNT(DOCUMENTS) FROM " + TEST_CORPUS + 
                         " WHERE CONTAINS(\"history\") ORDER BY date DESC LIMIT 10");
    }
    
    @Test
    @DisplayName("Complex query combining multiple features")
    void complexQuery() {
        executeAndVerify("SELECT ?person, ?date FROM " + TEST_CORPUS + 
                         " WHERE CONTAINS(\"Nobel Prize\") " +
                         "AND NER(\"PERSON\") AS ?person " +
                         "AND DATE(> 1900) AS ?date " +
                         "ORDER BY date DESC " +
                         "LIMIT 20");
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT COUNT(DOCUMENTS) FROM {corpus} WHERE CONTAINS(\"climate change\")",
        "SELECT ?location FROM {corpus} WHERE NER(\"LOCATION\") AS ?location AND CONTAINS(\"capital city\")",
        "SELECT ?person, ?org FROM {corpus} WHERE NER(\"PERSON\") AS ?person AND NER(\"ORGANIZATION\") AS ?org",
        "SELECT COUNT(DOCUMENTS) FROM {corpus} WHERE DATE(CONTAINS [1900, 2000])",
        "SELECT ?date FROM {corpus} WHERE DATE(> 1950) AS ?date AND CONTAINS(\"computer science\")",
        "SELECT COUNT(DOCUMENTS) FROM {corpus} WHERE DEPENDS(\"government\", \"nsubj\", \"announced\")",
        "SELECT COUNT(DOCUMENTS) FROM {corpus} WHERE (CONTAINS(\"economy\") OR CONTAINS(\"finance\")) AND NER(\"ORGANIZATION\")"
    })
    @DisplayName("Parameterized query tests")
    void parameterizedQueries(String queryTemplate) {
        String query = queryTemplate.replace("{corpus}", TEST_CORPUS);
        executeAndVerify(query);
    }
} 
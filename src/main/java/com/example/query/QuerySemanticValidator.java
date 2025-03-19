package com.example.query;

import com.example.query.model.*;
import com.example.query.binding.VariableRegistry;
import com.example.query.binding.Variable;
import com.example.query.binding.VariableType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Validates the semantic correctness of a query using the new VariableRegistry system.
 * This validator relies entirely on the VariableRegistry for validation rather than
 * keeping its own state of variable bindings.
 */
public class QuerySemanticValidator {
    private static final Logger logger = LoggerFactory.getLogger(QuerySemanticValidator.class);
    
    // Maximum allowed snippet window size (sentences)
    private static final int MAX_SNIPPET_WINDOW_SIZE = 5;

    /**
     * Validates a query for semantic correctness.
     *
     * @param query The query to validate
     * @throws QueryParseException if the query has semantic errors
     */
    public void validate(Query query) throws QueryParseException {
        logger.debug("Starting semantic validation for query: {}", query);
        
        // Get the variable registry from the query
        VariableRegistry registry = query.variableRegistry();
        
        // The registry's validate method already checks that all consumed variables are produced
        Set<String> registryErrors = registry.validate();
        if (!registryErrors.isEmpty()) {
            throw new QueryParseException("Variable validation errors: " + String.join(", ", registryErrors));
        }
        
        // Validate select columns
        validateSelectColumns(query, registry);
        
        // Validate limit value
        query.limit().ifPresent(limit -> {
            try {
                validateLimit(limit);
            } catch (QueryParseException e) {
                throw new RuntimeException(e);
            }
        });
        
        // Validate snippet window sizes
        validateSnippetWindowSizes(query);
        
        logger.debug("Semantic validation completed successfully");
    }
    
    /**
     * Validates the select columns, ensuring all column references are valid.
     */
    private void validateSelectColumns(Query query, VariableRegistry registry) throws QueryParseException {
        if (query.selectColumns().isEmpty()) {
            throw new QueryParseException("Query must select at least one column");
        }
        
        Set<String> allVariables = registry.getAllVariableNames();
        
        for (SelectColumn column : query.selectColumns()) {
            if (column instanceof VariableColumn variableColumn) {
                String variableName = Variable.formatName(variableColumn.getVariableName());
                
                logger.debug("Validating SELECT variable: {} against registry variables: {}", 
                             variableName, allVariables);
                
                if (!allVariables.contains(variableName)) {
                    throw new QueryParseException(String.format(
                        "Unbound variable in SELECT: %s. Variables must be bound in WHERE clause.",
                        variableName
                    ));
                }
                
                // Check that the variable is produced
                if (!registry.isProduced(variableName)) {
                    throw new QueryParseException(String.format(
                        "Variable %s in SELECT is consumed but not produced in any condition",
                        variableName
                    ));
                }
                
            } else if (column instanceof SnippetColumn snippetColumn) {
                validateSnippetNode(snippetColumn.getSnippetNode(), registry);
            }
            // Other column types (TITLE, TIMESTAMP, COUNT) don't need special validation
        }
    }
    
    /**
     * Validates a snippet node to ensure the variable is bound and window size is valid.
     */
    private void validateSnippetNode(SnippetNode snippetNode, VariableRegistry registry) throws QueryParseException {
        String variable = Variable.formatName(snippetNode.variable());
        
        // Check if variable is bound
        if (!registry.isProduced(variable)) {
            throw new QueryParseException(String.format(
                "Unbound variable in SNIPPET: %s. Variables must be bound in WHERE clause.",
                variable
            ));
        }
    }
    
    /**
     * Validates that all snippet window sizes are within acceptable limits.
     */
    private void validateSnippetWindowSizes(Query query) throws QueryParseException {
        for (SelectColumn column : query.selectColumns()) {
            if (column instanceof SnippetColumn snippetColumn) {
                SnippetNode snippetNode = snippetColumn.getSnippetNode();
                int windowSize = snippetNode.windowSize();
                
                if (windowSize > MAX_SNIPPET_WINDOW_SIZE) {
                    throw new QueryParseException(String.format(
                        "Snippet window size %d exceeds maximum allowed size of %d sentences",
                        windowSize, MAX_SNIPPET_WINDOW_SIZE
                    ));
                }
            }
        }
    }
    
    /**
     * Validates the limit value.
     */
    private void validateLimit(int limit) throws QueryParseException {
        if (limit <= 0) {
            throw new QueryParseException("LIMIT value must be greater than 0");
        }
    }
} 
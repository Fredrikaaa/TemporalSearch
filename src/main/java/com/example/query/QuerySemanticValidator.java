package com.example.query;

import com.example.query.model.*;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Dependency;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Ner;
import com.example.query.model.condition.Not;
import com.example.query.model.condition.Temporal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Validates the semantic correctness of a query.
 * This includes:
 * - Variable scoping and type checking
 * - Temporal date validation
 * - Entity type validation
 * - Dependency relation validation
 * - Snippet variable binding validation
 */
public class QuerySemanticValidator {
    private static final Logger logger = LoggerFactory.getLogger(QuerySemanticValidator.class);
    private static final Set<String> VALID_NER_TYPES = new HashSet<>(Arrays.asList(
        "PERSON", "ORGANIZATION", "LOCATION", "DATE", "TIME", "MONEY", "PERCENT",
        "NUMBER", "ORDINAL", "DURATION", "SET", "MISC"
    ));
    
    // Maximum allowed snippet window size (sentences)
    private static final int MAX_SNIPPET_WINDOW_SIZE = 5;

    private final Map<String, String> variableTypes = new HashMap<>();
    private final Set<String> boundVariables = new HashSet<>();

    /**
     * Helper method to handle checked exceptions in lambda expressions
     * while preserving the original exception type.
     */
    private static <T> Consumer<T> handleChecked(CheckedConsumer<T> consumer) {
        return t -> {
            try {
                consumer.accept(t);
            } catch (QueryParseException e) {
                // Rethrow the original exception without wrapping
                throw new QueryValidationException(e);
            }
        };
    }

    /**
     * Validates a query for semantic correctness.
     *
     * @param query The query to validate
     * @throws QueryParseException if the query has semantic errors
     */
    public void validate(Query query) throws QueryParseException {
        try {
            logger.debug("Starting semantic validation for query: {}", query);
            
            // Reset state
            variableTypes.clear();
            boundVariables.clear();
            
            // First pass: collect bound variables from conditions
            for (Condition condition : query.conditions()) {
                collectBoundVariables(condition);
            }
            
            // Second pass: validate conditions
            for (Condition condition : query.conditions()) {
                validateCondition(condition);
            }
            
            // Validate select list
            validateSelectColumns(query);
            
            // Validate order by
            for (String orderColumn : query.orderBy()) {
                validateOrderColumn(orderColumn);
            }
            
            // Validate limit
            query.limit().ifPresent(handleChecked(this::validateLimit));
            
            logger.debug("Semantic validation completed successfully");
        } catch (QueryValidationException e) {
            // Unwrap and rethrow the original exception
            throw (QueryParseException) e.getCause();
        }
    }
    
    /**
     * First pass to collect all bound variables for later validation.
     */
    private void collectBoundVariables(Condition condition) {
        if (condition instanceof Ner nerCondition) {
            if (nerCondition.isVariable()) {
                // Add with ? prefix for consistency
                boundVariables.add("?" + nerCondition.variableName());
            }
        } else if (condition instanceof Logical logicalCondition) {
            for (Condition subCondition : logicalCondition.conditions()) {
                collectBoundVariables(subCondition);
            }
        } else if (condition instanceof Not notCondition) {
            collectBoundVariables(notCondition.condition());
        }
        // Other condition types that might bind variables can be added here
    }
    
    /**
     * Validates the select columns, ensuring all column references are valid.
     */
    private void validateSelectColumns(Query query) throws QueryParseException {
        if (query.selectColumns().isEmpty()) {
            throw new QueryParseException("Query must select at least one column");
        }
        
        for (SelectColumn column : query.selectColumns()) {
            if (column instanceof SnippetColumn snippetColumn) {
                validateSnippetNode(snippetColumn.getSnippetNode());
            } else if (column instanceof VariableColumn variableColumn) {
                String variable = "?" + variableColumn.getVariableName();
                if (!boundVariables.contains(variable)) {
                    throw new QueryParseException(String.format(
                        "Unbound variable in SELECT: %s. Variables must be bound in WHERE clause.",
                        variable
                    ));
                }
            }
            // Other column types (TITLE, TIMESTAMP, COUNT) don't need special validation
        }
    }
    
    /**
     * Validates a snippet node to ensure the variable is bound and window size is valid.
     */
    private void validateSnippetNode(SnippetNode snippetNode) throws QueryParseException {
        String variable = snippetNode.variable();
        
        // Check if variable is bound
        if (!boundVariables.contains(variable)) {
            throw new QueryParseException(String.format(
                "Unbound variable in SNIPPET: %s. Variables must be bound in WHERE clause.",
                variable
            ));
        }
        
        // Check window size constraints
        int windowSize = snippetNode.windowSize();
        if (windowSize > MAX_SNIPPET_WINDOW_SIZE) {
            throw new QueryParseException(String.format(
                "Snippet window size %d exceeds maximum allowed size of %d sentences",
                windowSize, MAX_SNIPPET_WINDOW_SIZE
            ));
        }
    }

    private void validateCondition(Condition condition) throws QueryParseException {
        if (condition instanceof Contains) {
            validateContainsCondition((Contains) condition);
        } else if (condition instanceof Ner) {
            validateNerCondition((Ner) condition);
        } else if (condition instanceof Temporal) {
            validateTemporalCondition((Temporal) condition);
        } else if (condition instanceof Dependency) {
            validateDependencyCondition((Dependency) condition);
        } else if (condition instanceof Logical) {
            for (Condition subCondition : ((Logical) condition).conditions()) {
                validateCondition(subCondition);
            }
        } else if (condition instanceof Not) {
            validateCondition(((Not) condition).condition());
        }
    }

    private void validateContainsCondition(Contains condition) throws QueryParseException {
        if (condition.value().isEmpty()) {
            throw new QueryParseException("CONTAINS condition cannot have an empty value");
        }
    }

    private void validateNerCondition(Ner condition) throws QueryParseException {
        // Validate NER type
        if (!VALID_NER_TYPES.contains(condition.entityType().toUpperCase())) {
            throw new QueryParseException(String.format(
                "Invalid NER type: %s. Valid types are: %s",
                condition.entityType(),
                String.join(", ", VALID_NER_TYPES)
            ));
        }

        // Handle variable scoping
        if (condition.isVariable()) {
            String varName = condition.variableName();
            if (variableTypes.containsKey(varName)) {
                // Variable already exists, check type compatibility
                String existingType = variableTypes.get(varName);
                if (!existingType.equals(condition.entityType())) {
                    throw new QueryParseException(String.format(
                        "Type mismatch for variable %s: previously defined as %s, now used as %s",
                        varName, existingType, condition.entityType()
                    ));
                }
            } else {
                // New variable, register its type
                variableTypes.put(varName, condition.entityType());
            }
        }
    }

    private void validateTemporalCondition(Temporal condition) throws QueryParseException {
        try {
            validateDateTime(condition.startDate());
            condition.endDate().ifPresent(handleChecked(this::validateDateTime));

            // Always validate start/end dates if both are present
            if (condition.endDate().isPresent() && condition.startDate().isAfter(condition.endDate().get())) {
                throw new QueryParseException("End date must be after start date");
            }

            // For BETWEEN, ensure both dates are present
            if (condition.temporalType() == Temporal.Type.BETWEEN && !condition.endDate().isPresent()) {
                throw new QueryParseException("BETWEEN requires both start and end dates");
            }
            
            // Validate year values for date comparison
            if (condition.temporalType() == Temporal.Type.BEFORE || 
                condition.temporalType() == Temporal.Type.AFTER || 
                condition.temporalType() == Temporal.Type.BEFORE_EQUAL || 
                condition.temporalType() == Temporal.Type.AFTER_EQUAL || 
                condition.temporalType() == Temporal.Type.EQUAL) {
                int year = extractYearFromDate(condition.startDate());
                if (year < 0) {
                    throw new QueryParseException("Year values must be non-negative");
                }
            }
        } catch (DateTimeParseException e) {
            throw new QueryParseException("Invalid date format: " + e.getMessage());
        } catch (QueryValidationException e) {
            // Unwrap and rethrow the original exception
            throw (QueryParseException) e.getCause();
        }
    }
    
    /**
     * Helper method to extract year from LocalDateTime
     */
    private int extractYearFromDate(LocalDateTime dateTime) {
        return dateTime.getYear();
    }

    private void validateDateTime(LocalDateTime dateTime) throws QueryParseException {
        // Add any additional date validation logic here
        if (dateTime.isAfter(LocalDateTime.now())) {
            logger.warn("Query contains future date: {}", dateTime);
        }
    }

    private void validateDependencyCondition(Dependency condition) throws QueryParseException {
        // Could add validation of known dependency relations here
        if (condition.governor() == null || condition.governor().isEmpty() ||
            condition.relation() == null || condition.relation().isEmpty() ||
            condition.dependent() == null || condition.dependent().isEmpty()) {
            throw new QueryParseException("Empty dependency component");
        }
    }

    /**
     * Validates an order column.
     *
     * @param orderColumn The order column to validate
     * @throws QueryParseException if the order column is invalid
     */
    private void validateOrderColumn(String orderColumn) throws QueryParseException {
        // Remove the minus sign for descending order if present
        String columnName = orderColumn.startsWith("-") ? orderColumn.substring(1) : orderColumn;
        
        // Check if the column name is empty
        if (columnName == null || columnName.trim().isEmpty()) {
            throw new QueryParseException("Empty order by field");
        }
        
        // If this is a variable, check that it's bound
        if (columnName.startsWith("?")) {
            logger.debug("Validating ORDER BY variable: {} against bound variables: {}", columnName, boundVariables);
            
            if (!boundVariables.contains(columnName)) {
                throw new QueryParseException(String.format(
                    "Unbound variable in ORDER BY: %s. Variables must be bound in WHERE clause.",
                    columnName
                ));
            }
        }
    }

    private void validateLimit(int limit) throws QueryParseException {
        if (limit < 0) {
            throw new QueryParseException("Invalid limit");
        }
    }

    /**
     * Functional interface for operations that may throw QueryParseException
     */
    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws QueryParseException;
    }

    /**
     * Runtime exception used to wrap and propagate QueryParseException through lambda expressions
     */
    private static class QueryValidationException extends RuntimeException {
        QueryValidationException(Throwable cause) {
            super(cause);
        }
    }
} 
package com.example.query;

import com.example.query.model.*;
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
            for (Condition condition : query.getConditions()) {
                collectBoundVariables(condition);
            }
            
            // Second pass: validate conditions
            for (Condition condition : query.getConditions()) {
                validateCondition(condition);
            }
            
            // Validate select list (especially snippets)
            validateSelectColumns(query);
            
            // Validate order by
            for (OrderSpec orderSpec : query.getOrderBy()) {
                validateOrderSpec(orderSpec);
            }
            
            // Validate limit
            query.getLimit().ifPresent(handleChecked(this::validateLimit));
            
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
        if (condition instanceof NerCondition nerCondition) {
            if (nerCondition.isVariable()) {
                // Strip the '?' prefix if it exists
                String varName = nerCondition.getTarget();
                if (varName.startsWith("?")) {
                    // The target already includes the ? prefix in test code
                    boundVariables.add(varName);
                } else {
                    // Add with ? prefix for consistency
                    boundVariables.add("?" + varName);
                }
            }
        } else if (condition instanceof LogicalCondition logicalCondition) {
            for (Condition subCondition : logicalCondition.getConditions()) {
                collectBoundVariables(subCondition);
            }
        } else if (condition instanceof NotCondition notCondition) {
            collectBoundVariables(notCondition.getCondition());
        }
        // Other condition types that might bind variables can be added here
    }
    
    /**
     * Validates the select columns, with special attention to snippet nodes.
     */
    private void validateSelectColumns(Query query) throws QueryParseException {
        // This would need to be implemented after adding select columns to the Query model
        // For now, we'll assume any validation would happen at model creation time
        
        // Example validation for snippets if we had access to them:
        /*
        for (SelectColumn column : query.getSelectColumns()) {
            if (column instanceof SnippetNode snippetNode) {
                validateSnippetNode(snippetNode);
            }
        }
        */
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
        if (condition instanceof ContainsCondition) {
            validateContainsCondition((ContainsCondition) condition);
        } else if (condition instanceof NerCondition) {
            validateNerCondition((NerCondition) condition);
        } else if (condition instanceof TemporalCondition) {
            validateTemporalCondition((TemporalCondition) condition);
        } else if (condition instanceof DependencyCondition) {
            validateDependencyCondition((DependencyCondition) condition);
        } else if (condition instanceof LogicalCondition) {
            for (Condition subCondition : ((LogicalCondition) condition).getConditions()) {
                validateCondition(subCondition);
            }
        } else if (condition instanceof NotCondition) {
            validateCondition(((NotCondition) condition).getCondition());
        }
    }

    private void validateContainsCondition(ContainsCondition condition) throws QueryParseException {
        if (condition.getValue().isEmpty()) {
            throw new QueryParseException("CONTAINS condition cannot have an empty value");
        }
    }

    private void validateNerCondition(NerCondition condition) throws QueryParseException {
        // Validate NER type
        if (!VALID_NER_TYPES.contains(condition.getEntityType().toUpperCase())) {
            throw new QueryParseException(String.format(
                "Invalid NER type: %s. Valid types are: %s",
                condition.getEntityType(),
                String.join(", ", VALID_NER_TYPES)
            ));
        }

        // Handle variable scoping
        if (condition.isVariable()) {
            String varName = condition.getTarget();
            if (variableTypes.containsKey(varName)) {
                // Variable already exists, check type compatibility
                String existingType = variableTypes.get(varName);
                if (!existingType.equals(condition.getEntityType())) {
                    throw new QueryParseException(String.format(
                        "Type mismatch for variable %s: previously defined as %s, now used as %s",
                        varName, existingType, condition.getEntityType()
                    ));
                }
            } else {
                // New variable, register its type
                variableTypes.put(varName, condition.getEntityType());
            }
        }
    }

    private void validateTemporalCondition(TemporalCondition condition) throws QueryParseException {
        try {
            validateDateTime(condition.getStartDate());
            condition.getEndDate().ifPresent(handleChecked(this::validateDateTime));

            // For BETWEEN, ensure start is before end
            if (condition.getTemporalType() == TemporalCondition.Type.BETWEEN) {
                if (!condition.getEndDate().isPresent()) {
                    throw new QueryParseException("BETWEEN requires both start and end dates");
                }
                if (condition.getStartDate().isAfter(condition.getEndDate().get())) {
                    throw new QueryParseException("Start date must be before end date in BETWEEN condition");
                }
            }
            
            // Validate year values for date comparison
            if (condition.getTemporalType() == TemporalCondition.Type.BEFORE || 
                condition.getTemporalType() == TemporalCondition.Type.AFTER || 
                condition.getTemporalType() == TemporalCondition.Type.BEFORE_EQUAL || 
                condition.getTemporalType() == TemporalCondition.Type.AFTER_EQUAL || 
                condition.getTemporalType() == TemporalCondition.Type.EQUAL) {
                int year = extractYearFromDate(condition.getStartDate());
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

    private void validateDependencyCondition(DependencyCondition condition) throws QueryParseException {
        // Could add validation of known dependency relations here
        if (condition.getGovernor().isEmpty() || condition.getRelation().isEmpty() || condition.getDependent().isEmpty()) {
            throw new QueryParseException("Dependency condition cannot have empty components");
        }
    }

    private void validateOrderSpec(OrderSpec orderSpec) throws QueryParseException {
        // Add validation for valid ordering fields
        if (orderSpec.getField().isEmpty()) {
            throw new QueryParseException("Order by field cannot be empty");
        }
        
        // If this is a variable, check that it's bound
        if (orderSpec.getField().startsWith("?")) {
            String varName = orderSpec.getField();
            
            logger.debug("Validating ORDER BY variable: {} against bound variables: {}", varName, boundVariables);
            
            if (!boundVariables.contains(varName)) {
                throw new QueryParseException(String.format(
                    "Unbound variable in ORDER BY: %s. Variables must be bound in WHERE clause.",
                    varName
                ));
            }
        }
    }

    private void validateLimit(int limit) throws QueryParseException {
        if (limit <= 0) {
            throw new QueryParseException("Limit must be greater than 0");
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
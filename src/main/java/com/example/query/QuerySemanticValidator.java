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
 */
public class QuerySemanticValidator {
    private static final Logger logger = LoggerFactory.getLogger(QuerySemanticValidator.class);
    private static final Set<String> VALID_NER_TYPES = new HashSet<>(Arrays.asList(
        "PERSON", "ORGANIZATION", "LOCATION", "DATE", "TIME", "MONEY", "PERCENT",
        "NUMBER", "ORDINAL", "DURATION", "SET", "MISC"
    ));

    private final Map<String, String> variableTypes = new HashMap<>();

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
            
            // Validate conditions
            for (Condition condition : query.getConditions()) {
                validateCondition(condition);
            }
            
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

    private void validateCondition(Condition condition) throws QueryParseException {
        if (condition instanceof ContainsCondition) {
            validateContainsCondition((ContainsCondition) condition);
        } else if (condition instanceof NerCondition) {
            validateNerCondition((NerCondition) condition);
        } else if (condition instanceof TemporalCondition) {
            validateTemporalCondition((TemporalCondition) condition);
        } else if (condition instanceof DependencyCondition) {
            validateDependencyCondition((DependencyCondition) condition);
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
        } catch (DateTimeParseException e) {
            throw new QueryParseException("Invalid date format: " + e.getMessage());
        } catch (QueryValidationException e) {
            // Unwrap and rethrow the original exception
            throw (QueryParseException) e.getCause();
        }
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
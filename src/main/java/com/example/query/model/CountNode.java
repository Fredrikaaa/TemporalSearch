package com.example.query.model;

import java.util.Optional;

/**
 * Represents a COUNT aggregation expression in the SELECT clause of a query.
 * This expression performs counting operations on the result set.
 */
public record CountNode(
    CountType type,
    Optional<String> variable
) implements SelectColumn {
    
    /**
     * Enum representing the different types of COUNT operations.
     */
    public enum CountType {
        ALL,       // COUNT(*)
        UNIQUE,    // COUNT(UNIQUE ?var)
        DOCUMENTS  // COUNT(DOCUMENTS)
    }
    
    /**
     * Creates a COUNT(*) node.
     */
    public static CountNode countAll() {
        return new CountNode(CountType.ALL, Optional.empty());
    }
    
    /**
     * Creates a COUNT(UNIQUE ?var) node.
     */
    public static CountNode countUnique(String variable) {
        if (variable == null || variable.isEmpty()) {
            throw new IllegalArgumentException("variable must not be null or empty");
        }
        return new CountNode(CountType.UNIQUE, Optional.of(variable));
    }
    
    /**
     * Creates a COUNT(DOCUMENTS) node.
     */
    public static CountNode countDocuments() {
        return new CountNode(CountType.DOCUMENTS, Optional.empty());
    }
    
    /**
     * Validates that the node is well-formed.
     */
    public CountNode {
        if (type == CountType.UNIQUE && (variable.isEmpty() || variable.get().isEmpty())) {
            throw new IllegalArgumentException("UNIQUE count requires a non-empty variable");
        }
        if (type != CountType.UNIQUE && variable.isPresent()) {
            throw new IllegalArgumentException("Only UNIQUE count should have a variable");
        }
    }
    
    @Override
    public String toString() {
        return switch (type) {
            case ALL -> "COUNT(*)";
            case UNIQUE -> "COUNT(UNIQUE " + variable.get() + ")";
            case DOCUMENTS -> "COUNT(DOCUMENTS)";
        };
    }
} 
package com.example.query.model;

import no.ntnu.sandbox.Nash;

/**
 * Unified predicates for temporal relationships between date ranges.
 * Used throughout the system for both direct temporal conditions and join operations.
 */
public enum TemporalPredicate {
    // Basic comparison operators
    BEFORE,        // <
    AFTER,         // >
    BEFORE_EQUAL,  // <=
    AFTER_EQUAL,   // >=
    EQUAL,         // ==
    
    // Core temporal relationships
    /**
     * Date range completely contains another date range.
     * For example: [2023-01-01, 2024-12-31] CONTAINS [2023-05-01, 2023-12-31]
     */
    CONTAINS,
    
    /**
     * Date range is completely contained by another date range.
     * For example: [2023-05-01, 2023-12-31] CONTAINED_BY [2023-01-01, 2024-12-31]
     */
    CONTAINED_BY,
    
    /**
     * Date ranges have any overlap with each other.
     * For example: [2023-01-01, 2023-06-30] INTERSECT [2023-05-01, 2023-12-31]
     */
    INTERSECT,
    
    /**
     * Dates are within a specified time window of each other.
     * For example: 2023-01-01 PROXIMITY 2023-01-03 (within 2 days)
     */
    PROXIMITY;
    
    /**
     * Maps this TemporalPredicate to the corresponding Nash.RangePredicate.
     * 
     * @return The Nash predicate that corresponds to this temporal predicate
     */
    public Nash.RangePredicate toNashPredicate() {
        return switch (this) {
            case CONTAINS -> Nash.RangePredicate.CONTAINS;
            case CONTAINED_BY -> Nash.RangePredicate.CONTAINED_BY;
            case INTERSECT -> Nash.RangePredicate.INTERSECT;
            case PROXIMITY -> Nash.RangePredicate.PROXIMITY;
            
            // For these other temporal types, we map to the most appropriate Nash predicate
            case BEFORE, BEFORE_EQUAL -> Nash.RangePredicate.INTERSECT;
            case AFTER, AFTER_EQUAL -> Nash.RangePredicate.INTERSECT;
            case EQUAL -> Nash.RangePredicate.INTERSECT;
        };
    }
    
    /**
     * Checks if this predicate is a basic comparison operator.
     */
    public boolean isComparisonOperator() {
        return this == BEFORE || this == AFTER || 
               this == BEFORE_EQUAL || this == AFTER_EQUAL || 
               this == EQUAL;
    }
    
    /**
     * Checks if this predicate requires a date range (rather than a single date).
     */
    public boolean requiresDateRange() {
        return this == CONTAINS || this == CONTAINED_BY || this == INTERSECT;
    }
    
    /**
     * Converts from a comparison operator string to the appropriate predicate.
     */
    public static TemporalPredicate fromComparisonOperator(String operator) {
        return switch (operator) {
            case "<" -> BEFORE;
            case ">" -> AFTER;
            case "<=" -> BEFORE_EQUAL;
            case ">=" -> AFTER_EQUAL;
            case "==", "=" -> EQUAL;
            default -> throw new IllegalArgumentException("Invalid comparison operator: " + operator);
        };
    }
} 
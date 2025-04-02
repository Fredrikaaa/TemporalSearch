package com.example.query.model;

/**
 * Specialized predicates for temporal relationships between date ranges.
 * Used in join conditions for temporal joins between query result sets.
 */
public enum TemporalPredicate {
    /**
     * Left date range completely contains the right date range.
     */
    CONTAINS,
    
    /**
     * Left date range is completely contained by the right date range.
     */
    CONTAINED_BY,
    
    /**
     * Left and right date ranges overlap in any way.
     */
    INTERSECT,
    
    /**
     * Left and right dates are within a specified time window of each other.
     */
    PROXIMITY
} 
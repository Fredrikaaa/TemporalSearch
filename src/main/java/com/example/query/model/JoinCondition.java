package com.example.query.model;

import java.util.Objects;
import java.util.Optional;

/**
 * Defines how two result sets should be joined, specifically for temporal joins
 * between the main query and a subquery.
 */
public record JoinCondition(
    String leftColumn,
    String rightColumn,
    JoinType type,
    TemporalPredicate temporalPredicate,
    Optional<Integer> proximityWindow
) {
    /**
     * Type of join to perform.
     */
    public enum JoinType {
        INNER,
        LEFT,
        RIGHT
    }
    
    /**
     * Creates a join condition with validation.
     */
    public JoinCondition {
        Objects.requireNonNull(leftColumn, "Left column cannot be null");
        Objects.requireNonNull(rightColumn, "Right column cannot be null");
        Objects.requireNonNull(type, "Join type cannot be null");
        Objects.requireNonNull(temporalPredicate, "Temporal predicate cannot be null");
        Objects.requireNonNull(proximityWindow, "Proximity window cannot be null");
        
        // For PROXIMITY joins, the window size must be provided
        if (temporalPredicate == TemporalPredicate.PROXIMITY && proximityWindow.isEmpty()) {
            throw new IllegalArgumentException("Proximity window must be specified for PROXIMITY joins");
        }
        
        // For non-PROXIMITY joins, window should not be specified
        if (temporalPredicate != TemporalPredicate.PROXIMITY && proximityWindow.isPresent()) {
            throw new IllegalArgumentException("Proximity window should not be specified for non-PROXIMITY joins");
        }
    }
    
    /**
     * Creates a standard temporal join condition (without proximity).
     */
    public JoinCondition(String leftColumn, String rightColumn, JoinType type, TemporalPredicate temporalPredicate) {
        this(leftColumn, rightColumn, type, temporalPredicate, 
             temporalPredicate == TemporalPredicate.PROXIMITY ? 
                 Optional.of(1) : Optional.empty());
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leftColumn).append(" ");
        sb.append(temporalPredicate).append(" ");
        sb.append(rightColumn);
        
        proximityWindow.ifPresent(window -> 
            sb.append(" WINDOW ").append(window));
        
        return sb.toString();
    }
} 
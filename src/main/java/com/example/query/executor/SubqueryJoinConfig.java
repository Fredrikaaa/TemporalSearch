package com.example.query.executor;

import com.example.query.model.JoinCondition;
import com.example.query.model.TemporalPredicate;

import java.util.Optional;

/**
 * Configuration for joining subqueries with temporal joins.
 * This class bridges the gap between the query model and the join executor.
 */
public record SubqueryJoinConfig(
    String leftQueryName,
    String rightQueryName,
    String leftColumn,
    String rightColumn,
    JoinCondition.JoinType joinType,
    TemporalPredicate temporalPredicate,
    Optional<Integer> proximityWindow
) {
    /**
     * Creates a subquery join configuration with validation.
     */
    public SubqueryJoinConfig {
        if (leftQueryName == null || leftQueryName.isBlank()) {
            throw new IllegalArgumentException("Left query name cannot be null or blank");
        }
        if (rightQueryName == null || rightQueryName.isBlank()) {
            throw new IllegalArgumentException("Right query name cannot be null or blank");
        }
        if (leftColumn == null || leftColumn.isBlank()) {
            throw new IllegalArgumentException("Left column cannot be null or blank");
        }
        if (rightColumn == null || rightColumn.isBlank()) {
            throw new IllegalArgumentException("Right column cannot be null or blank");
        }
        if (joinType == null) {
            joinType = JoinCondition.JoinType.INNER;  // Default to INNER join
        }
        if (temporalPredicate == null) {
            throw new IllegalArgumentException("Temporal predicate cannot be null");
        }
        if (temporalPredicate == TemporalPredicate.PROXIMITY && 
            (proximityWindow == null || proximityWindow.isEmpty())) {
            throw new IllegalArgumentException("Proximity window must be specified for PROXIMITY joins");
        }
    }
    
    /**
     * Creates a join configuration without a proximity window.
     */
    public SubqueryJoinConfig(
            String leftQueryName, 
            String rightQueryName, 
            String leftColumn, 
            String rightColumn, 
            JoinCondition.JoinType joinType, 
            TemporalPredicate temporalPredicate) {
        this(leftQueryName, rightQueryName, leftColumn, rightColumn, joinType, temporalPredicate, 
             temporalPredicate == TemporalPredicate.PROXIMITY ? 
                 Optional.of(1) : Optional.empty());
    }
    
    /**
     * Creates a JoinCondition from this configuration.
     * 
     * @return A JoinCondition for use with JoinExecutor
     */
    public JoinCondition toJoinCondition() {
        return new JoinCondition(leftColumn, rightColumn, joinType, temporalPredicate, proximityWindow);
    }
    
    /**
     * Gets the name that should be used for the result table after joining.
     * 
     * @return A name for the joined result table
     */
    public String getResultTableName() {
        return leftQueryName + "_" + rightQueryName + "_join";
    }
} 
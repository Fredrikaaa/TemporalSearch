package com.example.query.model.condition;

/**
 * Root of the condition hierarchy for query expressions.
 * Each condition type represents a different kind of query constraint.
 * 
 * @see com.example.query.executor.ConditionExecutor
 */
public sealed interface Condition 
    permits Contains, 
            Dependency,
            Logical,
            Ner,
            Not,
            Pos,
            Temporal {
    
    /**
     * Gets the type of the condition.
     * Used for logging and error reporting.
     *
     * @return The condition type as a string
     */
    String getType();
} 
package com.example.query.model.condition;

import java.util.List;
import java.util.Objects;

/**
 * Represents a logical operation (AND, OR) between multiple conditions.
 */
public record Logical(
    LogicalOperator operator,
    List<Condition> conditions
) implements Condition {
    
    /**
     * The type of logical operation.
     */
    public enum LogicalOperator {
        AND,
        OR
    }
    
    /**
     * Creates a logical condition with validation.
     */
    public Logical {
        Objects.requireNonNull(operator, "operator cannot be null");
        Objects.requireNonNull(conditions, "conditions cannot be null");
        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("conditions cannot be empty");
        }
        // Make defensive copy of conditions
        conditions = List.copyOf(conditions);
    }
    
    /**
     * Creates a new logical condition with the specified operator and exactly two conditions.
     * 
     * @param operator The logical operator (AND, OR)
     * @param left The left condition
     * @param right The right condition
     */
    public Logical(LogicalOperator operator, Condition left, Condition right) {
        this(operator, List.of(
            Objects.requireNonNull(left, "left condition cannot be null"),
            Objects.requireNonNull(right, "right condition cannot be null")
        ));
    }
    
    @Override
    public String getType() {
        return operator.name();
    }
    
    @Override
    public String toString() {
        return String.format("LogicalCondition{operator=%s, conditions=%s}", operator, conditions);
    }
} 
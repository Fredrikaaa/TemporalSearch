package com.example.query.model.condition;

import java.util.Objects;

/**
 * Represents a logical negation (NOT) of a condition.
 */
public record Not(
    Condition condition
) implements Condition {
    
    /**
     * Creates a new NOT condition with validation.
     */
    public Not {
        Objects.requireNonNull(condition, "condition cannot be null");
    }
    
    @Override
    public String getType() {
        return "NOT";
    }
    
    @Override
    public String toString() {
        return String.format("NotCondition{condition=%s}", condition);
    }
} 
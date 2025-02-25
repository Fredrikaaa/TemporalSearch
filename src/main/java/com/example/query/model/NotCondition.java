package com.example.query.model;

/**
 * Represents a logical negation (NOT) of a condition.
 */
public class NotCondition implements Condition {
    
    private final Condition condition;
    
    /**
     * Creates a new NOT condition.
     * 
     * @param condition The condition to negate
     */
    public NotCondition(Condition condition) {
        if (condition == null) {
            throw new NullPointerException("condition cannot be null");
        }
        this.condition = condition;
    }
    
    /**
     * Returns the condition being negated.
     * 
     * @return The condition
     */
    public Condition getCondition() {
        return condition;
    }
    
    @Override
    public String getType() {
        return "NOT";
    }
    
    @Override
    public String toString() {
        return "NotCondition{condition=" + condition + "}";
    }
} 
package com.example.query.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Represents a logical operation (AND, OR) between multiple conditions.
 */
public class LogicalCondition implements Condition {
    
    /**
     * The type of logical operation.
     */
    public enum LogicalOperator {
        AND,
        OR
    }
    
    private final LogicalOperator operator;
    private final List<Condition> conditions;
    
    /**
     * Creates a new logical condition with the specified operator and conditions.
     * 
     * @param operator The logical operator (AND, OR)
     * @param conditions The conditions to combine
     */
    public LogicalCondition(LogicalOperator operator, List<Condition> conditions) {
        if (operator == null) {
            throw new NullPointerException("operator cannot be null");
        }
        if (conditions == null || conditions.isEmpty()) {
            throw new IllegalArgumentException("conditions cannot be null or empty");
        }
        
        this.operator = operator;
        this.conditions = new ArrayList<>(conditions);
    }
    
    /**
     * Creates a new logical condition with the specified operator and exactly two conditions.
     * 
     * @param operator The logical operator (AND, OR)
     * @param left The left condition
     * @param right The right condition
     */
    public LogicalCondition(LogicalOperator operator, Condition left, Condition right) {
        if (operator == null) {
            throw new NullPointerException("operator cannot be null");
        }
        if (left == null || right == null) {
            throw new NullPointerException("conditions cannot be null");
        }
        
        this.operator = operator;
        this.conditions = new ArrayList<>(2);
        this.conditions.add(left);
        this.conditions.add(right);
    }
    
    /**
     * Returns the logical operator.
     * 
     * @return The logical operator
     */
    public LogicalOperator getOperator() {
        return operator;
    }
    
    /**
     * Returns the conditions being combined.
     * 
     * @return Unmodifiable list of conditions
     */
    public List<Condition> getConditions() {
        return Collections.unmodifiableList(conditions);
    }
    
    @Override
    public String getType() {
        return operator.name();
    }
    
    @Override
    public String toString() {
        return "LogicalCondition{operator=" + operator + ", conditions=" + conditions + "}";
    }
} 
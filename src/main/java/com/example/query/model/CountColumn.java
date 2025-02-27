package com.example.query.model;

/**
 * Represents a COUNT expression in the SELECT clause of a query.
 * This column calculates a count of matches based on the count configuration.
 */
public class CountColumn implements SelectColumn {
    private final CountNode countNode;
    
    /**
     * Creates a new count column.
     * 
     * @param countNode The count node containing the count configuration
     */
    public CountColumn(CountNode countNode) {
        this.countNode = countNode;
    }
    
    /**
     * Gets the count node.
     * 
     * @return The count node
     */
    public CountNode getCountNode() {
        return countNode;
    }
    
    @Override
    public String toString() {
        return countNode.toString();
    }
} 
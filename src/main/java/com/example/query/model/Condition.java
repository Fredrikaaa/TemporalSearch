package com.example.query.model;

/**
 * Base interface for all query conditions.
 * Each specific condition type (CONTAINS, NER, etc.) will implement this interface.
 */
public interface Condition {
    /**
     * Returns a string representation of the condition type.
     * This is used for logging and debugging purposes.
     *
     * @return The condition type as a string
     */
    String getType();
} 
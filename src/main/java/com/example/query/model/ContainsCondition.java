package com.example.query.model;

/**
 * Represents a CONTAINS condition in the query language.
 * This condition checks if a document contains specific text.
 */
public class ContainsCondition implements Condition {
    private final String value;

    public ContainsCondition(String value) {
        if (value == null) {
            throw new NullPointerException("value cannot be null");
        }
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String getType() {
        return "CONTAINS";
    }

    @Override
    public String toString() {
        return "ContainsCondition{value='" + value + "'}";
    }
} 
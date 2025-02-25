package com.example.query.model;

import java.util.Objects;

/**
 * Represents an ordering specification in a query.
 * Specifies the field to order by and the direction (ascending/descending).
 */
public record OrderSpec(String field, Direction direction) {
    
    public enum Direction {
        ASC,
        DESC
    }

    /**
     * Creates a new ordering specification with the specified field and direction.
     *
     * @param field The field to order by
     * @param direction The direction to order in (ASC or DESC)
     */
    public OrderSpec {
        // Allow empty field - validation happens in QuerySemanticValidator
        field = field == null ? "" : field;  // Convert null to empty string
        Objects.requireNonNull(direction, "Direction cannot be null");
    }

    /**
     * Creates a new ordering specification with the specified field and ascending direction.
     *
     * @param field The field to order by
     */
    public OrderSpec(String field) {
        this(field, Direction.ASC);  // Default to ascending order
    }

    /**
     * Checks if the field is valid.
     * 
     * @return true if the field is not empty and meets validation requirements
     */
    public boolean isValid() {
        if (field == null || field.trim().isEmpty()) {
            return false;
        }
        
        // Check if the field is a variable reference (starts with ?)
        if (field.startsWith("?")) {
            // Variable references should be at least 2 characters long (? + at least one character)
            return field.length() >= 2;
        }
        
        return true;
    }

    /**
     * Gets the field to order by.
     *
     * @return The field name
     */
    public String getField() {
        return field;
    }

    /**
     * Gets the direction to order in.
     *
     * @return The direction (ASC or DESC)
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Validates that the field exists in the result table.
     *
     * @param table The result table to validate against
     * @throws IllegalArgumentException if the field does not exist in the table
     */
    public void validateAgainstTable(ResultTable table) {
        if (!table.getColumnNames().contains(field)) {
            throw new IllegalArgumentException("Order by field not found in result table: " + field);
        }
    }

    @Override
    public String toString() {
        return field + " " + direction;
    }
} 
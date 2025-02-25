package com.example.query.model;

import java.util.Objects;

/**
 * Represents an ordering specification in a query.
 * Specifies the field to order by and the direction (ascending/descending).
 */
public class OrderSpec {
    public enum Direction {
        ASC,
        DESC
    }

    private final String field;
    private final Direction direction;

    /**
     * Creates a new ordering specification with the specified field and direction.
     *
     * @param field The field to order by
     * @param direction The direction to order in (ASC or DESC)
     * @throws IllegalArgumentException if the field is null or empty
     */
    public OrderSpec(String field, Direction direction) {
        validateField(field);
        this.field = field;
        this.direction = Objects.requireNonNull(direction, "Direction cannot be null");
    }

    /**
     * Creates a new ordering specification with the specified field and ascending direction.
     *
     * @param field The field to order by
     * @throws IllegalArgumentException if the field is null or empty
     */
    public OrderSpec(String field) {
        this(field, Direction.ASC);  // Default to ascending order
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
     * Validates that the field is a valid column reference.
     *
     * @param field The field to validate
     * @throws IllegalArgumentException if the field is null or empty
     */
    private void validateField(String field) {
        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Order by field cannot be null or empty");
        }
        
        // Check if the field is a variable reference (starts with ?)
        if (field.startsWith("?")) {
            // Variable references should be at least 2 characters long (? + at least one character)
            if (field.length() < 2) {
                throw new IllegalArgumentException("Variable reference must have a name after '?'");
            }
        }
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
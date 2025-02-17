package com.example.query.model;

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

    public OrderSpec(String field, Direction direction) {
        this.field = field;
        this.direction = direction;
    }

    public OrderSpec(String field) {
        this(field, Direction.ASC);  // Default to ascending order
    }

    public String getField() {
        return field;
    }

    public Direction getDirection() {
        return direction;
    }

    @Override
    public String toString() {
        return field + " " + direction;
    }
} 
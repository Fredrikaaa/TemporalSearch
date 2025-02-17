package com.example.query.model;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Represents a temporal condition in the query language.
 * This condition matches documents based on temporal expressions.
 */
public class TemporalCondition implements Condition {
    public enum Type {
        BEFORE,
        AFTER,
        BETWEEN
    }

    private final Type type;
    private final LocalDateTime startDate;
    private final Optional<LocalDateTime> endDate;

    public TemporalCondition(Type type, LocalDateTime startDate) {
        this.type = type;
        this.startDate = startDate;
        this.endDate = Optional.empty();
    }

    public TemporalCondition(LocalDateTime startDate, LocalDateTime endDate) {
        this.type = Type.BETWEEN;
        this.startDate = startDate;
        this.endDate = Optional.of(endDate);
    }

    @Override
    public String getType() {
        return "TEMPORAL";
    }

    public Type getTemporalType() {
        return type;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }

    public Optional<LocalDateTime> getEndDate() {
        return endDate;
    }

    @Override
    public String toString() {
        if (type == Type.BETWEEN && endDate.isPresent()) {
            return String.format("TemporalCondition{type=%s, startDate=%s, endDate=%s}", 
                type, startDate, endDate.get());
        }
        return String.format("TemporalCondition{type=%s, date=%s}", 
            type, startDate);
    }
} 
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
        BETWEEN,
        NEAR
    }

    private final Type type;
    private final LocalDateTime startDate;
    private final Optional<LocalDateTime> endDate;
    private final Optional<String> variable;
    private final Optional<String> range;

    public TemporalCondition(Type type, LocalDateTime startDate) {
        this.type = type;
        this.startDate = startDate;
        this.endDate = Optional.empty();
        this.variable = Optional.empty();
        this.range = Optional.empty();
    }

    public TemporalCondition(LocalDateTime startDate, LocalDateTime endDate) {
        this.type = Type.BETWEEN;
        this.startDate = startDate;
        this.endDate = Optional.of(endDate);
        this.variable = Optional.empty();
        this.range = Optional.empty();
    }

    public TemporalCondition(Type type, LocalDateTime date, String range) {
        this.type = type;
        this.startDate = date;
        this.endDate = Optional.empty();
        this.variable = Optional.empty();
        this.range = Optional.of(range);
    }

    public TemporalCondition(String variable) {
        this.type = null;
        this.startDate = null;
        this.endDate = Optional.empty();
        this.variable = Optional.of(variable);
        this.range = Optional.empty();
    }

    public TemporalCondition(Type type, String variable, LocalDateTime compareDate) {
        this.type = type;
        this.startDate = compareDate;
        this.endDate = Optional.empty();
        this.variable = Optional.of(variable);
        this.range = Optional.empty();
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

    public Optional<String> getVariable() {
        return variable;
    }

    public Optional<String> getRange() {
        return range;
    }

    @Override
    public String toString() {
        if (variable.isPresent()) {
            if (type != null) {
                return String.format("TemporalCondition{type=%s, variable=%s, compareDate=%s}",
                    type, variable.get(), startDate);
            }
            return String.format("TemporalCondition{variable=%s}", variable.get());
        }
        if (type == Type.BETWEEN && endDate.isPresent()) {
            return String.format("TemporalCondition{type=%s, startDate=%s, endDate=%s}",
                type, startDate, endDate.get());
        }
        if (type == Type.NEAR && range.isPresent()) {
            return String.format("TemporalCondition{type=%s, date=%s, range=%s}",
                type, startDate, range.get());
        }
        return String.format("TemporalCondition{type=%s, date=%s}",
            type, startDate);
    }
} 
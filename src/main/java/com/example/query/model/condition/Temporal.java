package com.example.query.model.condition;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

import com.example.query.model.TemporalRange;

/**
 * Represents a temporal condition in the query language.
 * This condition matches documents based on temporal expressions.
 */
public record Temporal(
    LocalDateTime startDate,
    Optional<LocalDateTime> endDate,
    Optional<String> variable,
    Optional<TemporalRange> range,
    Type temporalType
) implements Condition {
    
    public enum Type {
        BEFORE,        // <
        AFTER,         // >
        BEFORE_EQUAL,  // <=
        AFTER_EQUAL,   // >=
        EQUAL,         // ==
        CONTAINS,      // CONTAINS
        CONTAINED_BY,  // CONTAINED_BY
        INTERSECT,     // INTERSECT
        NEAR,         // NEAR
        BETWEEN        // Date range between two points
    }
    
    /**
     * Maps comparison operators to TemporalCondition.Type
     */
    public enum ComparisonType {
        LT(Type.BEFORE),
        GT(Type.AFTER),
        LE(Type.BEFORE_EQUAL),
        GE(Type.AFTER_EQUAL),
        EQ(Type.EQUAL);
        
        private final Type temporalType;
        
        ComparisonType(Type temporalType) {
            this.temporalType = temporalType;
        }
        
        public Type getTemporalType() {
            return temporalType;
        }
    }
    
    /**
     * Creates a temporal condition with validation.
     */
    public Temporal {
        Objects.requireNonNull(startDate, "Start date cannot be null");
        endDate = endDate != null ? endDate : Optional.empty();
        variable = variable != null ? variable : Optional.empty();
        range = range != null ? range : Optional.empty();
        Objects.requireNonNull(temporalType, "Temporal type cannot be null");
    }
    
    /**
     * Constructor for simple date comparison with a variable.
     */
    public Temporal(ComparisonType comparisonType, String variable, int year) {
        this(LocalDateTime.of(year, 1, 1, 0, 0),
             Optional.empty(),
             Optional.of(variable),
             Optional.empty(),
             comparisonType.getTemporalType());
    }
    
    /**
     * Constructor for simple temporal condition.
     */
    public Temporal(Type type, LocalDateTime startDate) {
        this(startDate, Optional.empty(), Optional.empty(), Optional.empty(), type);
    }
    
    /**
     * Constructor for date range condition.
     */
    public Temporal(LocalDateTime startDate, LocalDateTime endDate) {
        this(startDate, Optional.of(endDate), Optional.empty(), Optional.empty(), Type.BETWEEN);
    }
    
    /**
     * Constructor for temporal condition with range.
     */
    public Temporal(Type type, LocalDateTime date, String range) {
        this(date, Optional.empty(), Optional.empty(), Optional.of(new TemporalRange(range)), type);
    }
    
    /**
     * Constructor for variable-only condition.
     */
    public Temporal(String variable) {
        this(null, null, Optional.empty(), Optional.empty(), null);
    }
    
    /**
     * Constructor for variable comparison condition.
     */
    public Temporal(Type type, String variable, LocalDateTime compareDate) {
        this(compareDate, Optional.empty(), Optional.of(variable), Optional.empty(), type);
    }
    
    /**
     * Constructor for variable comparison condition with range.
     */
    public Temporal(Type type, String variable, LocalDateTime compareDate, String range) {
        this(compareDate, Optional.empty(), Optional.of(variable), Optional.of(new TemporalRange(range)), type);
    }
    
    @Override
    public String getType() {
        return "TEMPORAL";
    }
    
    @Override
    public String toString() {
        return String.format("TEMPORAL(%s, %s%s%s)", 
            temporalType,
            startDate,
            endDate.map(d -> ", " + d).orElse(""),
            variable.map(v -> ", var=" + v).orElse("")
        );
    }
} 
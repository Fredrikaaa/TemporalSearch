package com.example.query.model.condition;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;
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
     * Constructor for simple date comparison.
     */
    public Temporal(ComparisonType comparisonType, int year) {
        this(LocalDateTime.of(year, 1, 1, 0, 0),
             Optional.empty(),
             Optional.empty(),
             Optional.empty(),
             comparisonType.getTemporalType());
    }
    
    /**
     * Constructor for simple date comparison with a variable.
     */
    public Temporal(ComparisonType comparisonType, int year, String variableName) {
        this(LocalDateTime.of(year, 1, 1, 0, 0),
             Optional.empty(),
             Optional.of(variableName),
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
     * Constructor for temporal condition with range and variable.
     */
    public Temporal(Type type, LocalDateTime date, Optional<TemporalRange> range, String variableName) {
        this(date, Optional.empty(), Optional.of(variableName), range, type);
    }
    
    @Override
    public String getType() {
        return "TEMPORAL";
    }
    
    @Override
    public Set<String> getProducedVariables() {
        return variable.isPresent() ? Set.of(variable.get()) : Collections.emptySet();
    }
    
    @Override
    public VariableType getProducedVariableType() {
        return VariableType.TEMPORAL;
    }
    
    @Override
    public void registerVariables(VariableRegistry registry) {
        if (variable.isPresent()) {
            registry.registerProducer(variable.get(), getProducedVariableType(), getType());
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DATE(");
        
        // Format based on temporal type
        switch (temporalType) {
            case BEFORE:
                sb.append("< ").append(startDate.getYear());
                break;
            case AFTER:
                sb.append("> ").append(startDate.getYear());
                break;
            case BEFORE_EQUAL:
                sb.append("<= ").append(startDate.getYear());
                break;
            case AFTER_EQUAL:
                sb.append(">= ").append(startDate.getYear());
                break;
            case EQUAL:
                sb.append("== ").append(startDate.getYear());
                break;
            case CONTAINS:
            case CONTAINED_BY:
            case INTERSECT:
                sb.append(temporalType.name()).append(" ");
                if (endDate.isPresent()) {
                    sb.append("[").append(startDate.getYear()).append(", ").append(endDate.get().getYear()).append("]");
                } else {
                    sb.append(startDate.getYear());
                }
                break;
            case NEAR:
                sb.append("NEAR ").append(startDate.getYear());
                range.ifPresent(r -> sb.append(" RADIUS ").append(r.toString()));
                break;
            case BETWEEN:
                if (endDate.isPresent()) {
                    sb.append("BETWEEN ").append(startDate.getYear()).append(" AND ").append(endDate.get().getYear());
                }
                break;
        }
        
        sb.append(")");
        
        // Add variable if present
        if (variable.isPresent()) {
            sb.append(" AS ?").append(variable.get());
        }
        
        return sb.toString();
    }
} 
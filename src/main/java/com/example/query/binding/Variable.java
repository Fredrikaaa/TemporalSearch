package com.example.query.binding;

import java.util.Collections;
import java.util.Set;

/**
 * Represents a variable in the query language.
 * Variables can be either producers (bind values) or consumers (use values).
 * A variable may be both a producer and consumer in different parts of the query.
 */
public sealed interface Variable permits ProducerVariable, ConsumerVariable {
    
    /**
     * Gets the name of the variable.
     *
     * @return The variable name, without the ? prefix
     */
    String getName();
    
    /**
     * Gets the variable type for type checking.
     * This allows semantic validation of variable usage.
     *
     * @return The variable's data type
     */
    VariableType getType();
    
    /**
     * Creates a variable name from an identifier (adds ? prefix if needed).
     *
     * @param name The variable name, with or without the ? prefix
     * @return The variable name with the ? prefix
     */
    static String formatName(String name) {
        if (name == null) {
            return null;
        }
        return name.startsWith("?") ? name : "?" + name;
    }
}

/**
 * Represents a variable that produces values through extraction.
 * For example, NER(PERSON) AS ?person produces person entities.
 */
record ProducerVariable(
    String name,
    VariableType type,
    String sourceConditionType,
    Set<String> producedBy
) implements Variable {
    
    /**
     * Creates a producer variable with validation.
     */
    public ProducerVariable {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Variable name cannot be null or blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("Variable type cannot be null");
        }
        if (sourceConditionType == null || sourceConditionType.isBlank()) {
            throw new IllegalArgumentException("Source condition type cannot be null or blank");
        }
        
        // Ensure defensive copies
        name = Variable.formatName(name);
        producedBy = producedBy != null ? 
            Collections.unmodifiableSet(Set.copyOf(producedBy)) : 
            Collections.emptySet();
    }
    
    /**
     * Creates a simple producer variable with a single producing condition.
     */
    public ProducerVariable(String name, VariableType type, String sourceConditionType) {
        this(name, type, sourceConditionType, Set.of(sourceConditionType));
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public VariableType getType() {
        return type;
    }
}

/**
 * Represents a variable that consumes values from producers.
 * For example, CONTAINS(?person, "spoke") uses values from the ?person variable.
 */
record ConsumerVariable(
    String name,
    VariableType type,
    String consumingConditionType,
    Set<String> consumedBy
) implements Variable {
    
    /**
     * Creates a consumer variable with validation.
     */
    public ConsumerVariable {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Variable name cannot be null or blank");
        }
        if (type == null) {
            throw new IllegalArgumentException("Variable type cannot be null");
        }
        if (consumingConditionType == null || consumingConditionType.isBlank()) {
            throw new IllegalArgumentException("Consuming condition type cannot be null or blank");
        }
        
        // Ensure defensive copies
        name = Variable.formatName(name);
        consumedBy = consumedBy != null ? 
            Collections.unmodifiableSet(Set.copyOf(consumedBy)) : 
            Collections.emptySet();
    }
    
    /**
     * Creates a simple consumer variable with a single consuming condition.
     */
    public ConsumerVariable(String name, VariableType type, String consumingConditionType) {
        this(name, type, consumingConditionType, Set.of(consumingConditionType));
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public VariableType getType() {
        return type;
    }
} 
package com.example.query.model.condition;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;

/**
 * Represents a Part of Speech (POS) condition in the query language.
 * This condition matches documents where a word has a specific POS tag.
 */
public record Pos(
    String posTag,
    String term,
    String variableName,
    boolean isVariable
) implements Condition {
    
    /**
     * Creates a new POS condition with validation.
     */
    public Pos {
        Objects.requireNonNull(posTag, "posTag cannot be null");
        Objects.requireNonNull(term, "term cannot be null");
        
        if (isVariable) {
            Objects.requireNonNull(variableName, "variableName cannot be null when isVariable is true");
        }
    }

    /**
     * Creates a new POS condition without variable binding.
     */
    public Pos(String posTag, String term) {
        this(posTag, term, null, false);
    }

    /**
     * Creates a new POS condition with variable binding.
     */
    public Pos(String posTag, String term, String variableName) {
        this(posTag, term, variableName, true);
    }

    /**
     * Returns whether this condition uses variable binding.
     */
    public boolean isVariable() {
        return isVariable;
    }

    /**
     * Returns the variable name if this is a variable binding condition.
     */
    public String getVariableName() {
        return variableName;
    }
    
    @Override
    public String getType() {
        return "POS";
    }
    
    @Override
    public Set<String> getProducedVariables() {
        return isVariable ? Set.of(variableName) : Collections.emptySet();
    }
    
    @Override
    public VariableType getProducedVariableType() {
        return VariableType.POS_TAG;
    }
    
    @Override
    public void registerVariables(VariableRegistry registry) {
        if (isVariable) {
            registry.registerProducer(variableName, getProducedVariableType(), getType());
        }
    }
    
    @Override
    public String toString() {
        if (isVariable) {
            return String.format("POS(%s, %s) AS ?%s", posTag, term, variableName);
        }
        return String.format("POS(%s, %s)", posTag, term);
    }
} 
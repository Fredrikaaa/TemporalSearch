package com.example.query.model.condition;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;

/**
 * Represents a POS (Part-of-Speech) condition in the query language.
 * This condition checks for terms with a specific POS tag, optionally binding the term.
 */
public record Pos(
    String posTag,
    String term,        // Term can be null when isVariable is true
    String variableName,
    boolean isVariable
) implements Condition {
    
    /**
     * Creates a condition with validation.
     */
    public Pos {
        Objects.requireNonNull(posTag, "posTag cannot be null");
        
        if (isVariable) {
            // When binding a variable, term can be null (extract any term with the tag)
            Objects.requireNonNull(variableName, "variableName cannot be null when isVariable is true");
        } else {
            // When searching for a specific term/tag combination, term must be provided
            Objects.requireNonNull(term, "term cannot be null when isVariable is false");
        }
        
        // No defensive copy needed for Strings
    }

    /**
     * Creates a condition with a term and POS tag (non-variable).
     *
     * @param posTag The part-of-speech tag
     * @param term The search term
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
        // Changed to reflect the "term/TAG" format - TEXT_SPAN is most appropriate
        return VariableType.TEXT_SPAN; 
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
            return String.format("POS(%s) AS %s", posTag, variableName);
        } else {
            return String.format("POS(%s, %s)", posTag, term);
        }
    }
} 
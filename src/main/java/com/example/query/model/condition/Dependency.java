package com.example.query.model.condition;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;

/**
 * Represents a dependency condition in the query language.
 * This condition matches documents based on syntactic dependencies between words.
 */
public record Dependency(
    String governor,
    String relation,
    String dependent,
    String variableName,
    boolean isVariable
) implements Condition {
    
    /**
     * Creates a new dependency condition with validation.
     */
    public Dependency {
        Objects.requireNonNull(governor, "governor cannot be null");
        Objects.requireNonNull(relation, "relation cannot be null");
        Objects.requireNonNull(dependent, "dependent cannot be null");
        
        if (isVariable) {
            Objects.requireNonNull(variableName, "variableName cannot be null when isVariable is true");
        }
    }

    /**
     * Creates a new dependency condition without variable binding.
     */
    public Dependency(String governor, String relation, String dependent) {
        this(governor, relation, dependent, null, false);
    }

    /**
     * Creates a new dependency condition with variable binding.
     * 
     * @param governor The governor term
     * @param relation The dependency relation
     * @param dependent The dependent term
     * @param variableName The variable name to bind the dependency to
     */
    public Dependency(String governor, String relation, String dependent, String variableName) {
        this(governor, relation, dependent, variableName, true);
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
        return "DEPENDENCY";
    }
    
    @Override
    public Set<String> getProducedVariables() {
        return isVariable ? Set.of(variableName) : Collections.emptySet();
    }
    
    @Override
    public VariableType getProducedVariableType() {
        return VariableType.DEPENDENCY;
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
            return String.format("DEPENDS(%s, %s, %s) AS ?%s", governor, relation, dependent, variableName);
        }
        return String.format("DEPENDS(%s, %s, %s)", governor, relation, dependent);
    }
} 
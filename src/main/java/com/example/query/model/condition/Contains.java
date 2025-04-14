package com.example.query.model.condition;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;

/**
 * Represents a CONTAINS condition in the query language.
 * This condition checks if a document contains specific text or multiple terms.
 */
public record Contains(
    List<String> terms,
    String variableName,
    boolean isVariable,
    String value
) implements Condition {
    
    /**
     * Creates a condition with validation.
     */
    public Contains {
        Objects.requireNonNull(terms, "Terms cannot be null");
        // Allow value to be null only if terms is empty
        if (!terms.isEmpty()) {
            Objects.requireNonNull(value, "Value cannot be null when terms is not empty");
        }
        // Removed check for empty terms list
        
        // Make defensive copy of terms
        terms = List.copyOf(terms);
        
        if (isVariable) {
            Objects.requireNonNull(variableName, "variableName cannot be null when isVariable is true");
        }
    }

    /**
     * Creates a condition with a single term.
     * 
     * @param term The search term
     */
    public Contains(String term) {
        this(Collections.singletonList(Objects.requireNonNull(term, "term cannot be null")), null, false, term);
    }
    
    /**
     * Creates a condition with multiple terms.
     * 
     * @param terms List of search terms
     */
    public Contains(List<String> terms) {
        // Pass null for value if terms is empty
        this(terms, null, false, terms.isEmpty() ? null : terms.get(0));
    }

    /**
     * Creates a condition with a variable binding and a term.
     * 
     * @param term The search term
     * @param variableName The variable name to bind results to
     * @param isVariable Whether this condition binds to a variable
     */
    public Contains(String term, String variableName, boolean isVariable) {
        this(Collections.singletonList(Objects.requireNonNull(term, "term cannot be null")), 
             variableName, isVariable, term);
    }

    /**
     * Returns the search terms.
     * 
     * @return Unmodifiable list of search terms
     */
    @Override
    public List<String> terms() {
        return terms; // Already unmodifiable from constructor
    }

    /**
     * Returns the first search term (for backward compatibility).
     * 
     * @return The first search term
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns whether this condition uses variable binding.
     * 
     * @return true if this condition binds to a variable, false otherwise
     */
    public boolean isVariable() {
        return isVariable;
    }

    /**
     * Returns the variable name if this is a variable binding condition.
     * 
     * @return The variable name, or null if this is not a variable binding condition
     */
    public String getVariableName() {
        return variableName;
    }

    @Override
    public String getType() {
        return "CONTAINS";
    }
    
    @Override
    public Set<String> getProducedVariables() {
        return isVariable ? Set.of(variableName) : Collections.emptySet();
    }
    
    @Override
    public VariableType getProducedVariableType() {
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
            return String.format("CONTAINS(%s) AS ?%s", String.join(", ", terms), variableName);
        }
        return String.format("CONTAINS(%s)", String.join(", ", terms));
    }
} 
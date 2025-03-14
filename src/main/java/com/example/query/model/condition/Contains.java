package com.example.query.model.condition;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        Objects.requireNonNull(value, "Value cannot be null");
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("terms cannot be empty");
        }
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
        this(terms, null, false, terms.get(0));
    }

    /**
     * Creates a condition with a variable binding and a term.
     * 
     * @param variableName The variable name to bind results to
     * @param term The search term
     */
    public Contains(String variableName, String term) {
        this(Collections.singletonList(Objects.requireNonNull(term, "term cannot be null")), 
             Objects.requireNonNull(variableName, "variableName cannot be null"), 
             true, term);
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
    public String toString() {
        return String.format("CONTAINS(%s)", String.join(", ", terms));
    }
} 
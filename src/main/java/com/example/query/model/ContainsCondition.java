package com.example.query.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a CONTAINS condition in the query language.
 * This condition checks if a document contains specific text or multiple terms.
 */
public class ContainsCondition implements Condition {
    private final List<String> terms;
    private final String variableName;
    private final boolean isVariable;

    /**
     * Creates a condition with a single term.
     * 
     * @param term The search term
     */
    public ContainsCondition(String term) {
        if (term == null) {
            throw new NullPointerException("term cannot be null");
        }
        this.terms = Collections.singletonList(term);
        this.variableName = null;
        this.isVariable = false;
    }
    
    /**
     * Creates a condition with multiple terms.
     * 
     * @param terms List of search terms
     */
    public ContainsCondition(List<String> terms) {
        if (terms == null) {
            throw new NullPointerException("terms cannot be null");
        }
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("terms cannot be empty");
        }
        this.terms = new ArrayList<>(terms);
        this.variableName = null;
        this.isVariable = false;
    }

    /**
     * Creates a condition with a variable binding and a term.
     * 
     * @param variableName The variable name to bind results to
     * @param term The search term
     */
    public ContainsCondition(String variableName, String term) {
        if (term == null) {
            throw new NullPointerException("term cannot be null");
        }
        if (variableName == null) {
            throw new NullPointerException("variableName cannot be null");
        }
        this.terms = Collections.singletonList(term);
        this.variableName = variableName;
        this.isVariable = true;
    }

    /**
     * Returns the search terms.
     * 
     * @return Unmodifiable list of search terms
     */
    public List<String> getTerms() {
        return Collections.unmodifiableList(terms);
    }
    
    /**
     * Returns the first search term (for backward compatibility).
     * 
     * @return The first search term
     */
    public String getValue() {
        return terms.get(0);
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
        if (isVariable) {
            return "ContainsCondition{variable='" + variableName + "', term='" + terms.get(0) + "'}";
        }
        if (terms.size() == 1) {
            return "ContainsCondition{value='" + terms.get(0) + "'}";
        }
        return "ContainsCondition{terms=" + terms + "}";
    }
} 
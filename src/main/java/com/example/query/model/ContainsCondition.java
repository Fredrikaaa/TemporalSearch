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

    @Override
    public String getType() {
        return "CONTAINS";
    }

    @Override
    public String toString() {
        if (terms.size() == 1) {
            return "ContainsCondition{value='" + terms.get(0) + "'}";
        }
        return "ContainsCondition{terms=" + terms + "}";
    }
} 
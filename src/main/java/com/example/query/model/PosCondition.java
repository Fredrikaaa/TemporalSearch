package com.example.query.model;

/**
 * Represents a Part-of-Speech (POS) condition in the query language.
 * This condition matches documents where words with specific POS tags are found.
 */
public class PosCondition implements Condition {
    private final String posTag;
    private final String term;
    private final boolean isVariable;

    /**
     * Creates a new POS condition.
     *
     * @param posTag The part-of-speech tag to search for (e.g., "NN", "VB", "JJ")
     * @param term The term to match or variable to bind
     * @param isVariable Whether the term is a variable (true) or literal (false)
     */
    public PosCondition(String posTag, String term, boolean isVariable) {
        if (posTag == null) {
            throw new NullPointerException("posTag cannot be null");
        }
        if (term == null) {
            throw new NullPointerException("term cannot be null");
        }
        this.posTag = posTag;
        this.term = term;
        this.isVariable = isVariable;
    }

    @Override
    public String getType() {
        return "POS";
    }

    /**
     * Returns the part-of-speech tag.
     *
     * @return The POS tag
     */
    public String getPosTag() {
        return posTag;
    }

    /**
     * Returns the term or variable name.
     *
     * @return The term or variable name
     */
    public String getTerm() {
        return term;
    }

    /**
     * Returns whether the term is a variable.
     *
     * @return true if the term is a variable, false if it's a literal
     */
    public boolean isVariable() {
        return isVariable;
    }

    @Override
    public String toString() {
        return String.format("PosCondition{posTag='%s', term='%s', isVariable=%s}", 
            posTag, term, isVariable);
    }
} 
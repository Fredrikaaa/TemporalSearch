package com.example.query.model;

/**
 * Represents a Named Entity Recognition (NER) condition in the query language.
 * This condition matches documents where a specific entity type is found.
 */
public class NerCondition implements Condition {
    private final String type;
    private final String target;
    private final boolean isVariable;

    public NerCondition(String type, String target, boolean isVariable) {
        if (type == null) {
            throw new NullPointerException("type cannot be null");
        }
        if (target == null) {
            throw new NullPointerException("target cannot be null");
        }
        this.type = type;
        this.target = target;
        this.isVariable = isVariable;
    }

    public String getType() {
        return "NER";
    }

    public String getEntityType() {
        return type;
    }

    public String getTarget() {
        return target;
    }

    public boolean isVariable() {
        return isVariable;
    }

    @Override
    public String toString() {
        return String.format("NerCondition{type='%s', target='%s', isVariable=%s}", 
            type, target, isVariable);
    }
} 
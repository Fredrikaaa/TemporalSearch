package com.example.query.model;

/**
 * Represents a dependency condition in the query language.
 * This condition matches documents based on syntactic dependencies between words.
 */
public class DependencyCondition implements Condition {
    private final String governor;
    private final String relation;
    private final String dependent;

    public DependencyCondition(String governor, String relation, String dependent) {
        this.governor = governor;
        this.relation = relation;
        this.dependent = dependent;
    }

    @Override
    public String getType() {
        return "DEPENDENCY";
    }

    public String getGovernor() {
        return governor;
    }

    public String getRelation() {
        return relation;
    }

    public String getDependent() {
        return dependent;
    }

    @Override
    public String toString() {
        return String.format("DependencyCondition{governor='%s', relation='%s', dependent='%s'}", 
            governor, relation, dependent);
    }
} 
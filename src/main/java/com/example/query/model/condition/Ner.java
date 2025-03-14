package com.example.query.model.condition;

import java.util.Objects;

/**
 * Represents a Named Entity Recognition (NER) condition in the query language.
 * This condition matches documents where a specific entity type is found.
 * 
 * Supported entity types (as per CoreNLP):
 * - PERSON: Person names
 * - ORGANIZATION: Organization names
 * - LOCATION: Location names
 * - DATE: Date expressions
 * - TIME: Time expressions
 * - DURATION: Duration expressions
 * - MONEY: Monetary amounts
 * - NUMBER: Numeric values
 * - ORDINAL: Ordinal numbers
 * - PERCENT: Percentage values
 * - SET: Set expressions (e.g., "weekly", "monthly")
 * 
 * Usage examples:
 * - NER("PERSON", "?person") - Binds person entities to variable
 * - NER("ORGANIZATION") - Matches any organization
 * - NER("DATE", "?date") - Binds date expressions to variable
 * - NER("MONEY") - Matches any monetary amount
 */
public record Ner(
    String entityType,
    String variableName
) implements Condition {
    
    /**
     * Creates a new NER condition with validation.
     */
    public Ner {
        Objects.requireNonNull(entityType, "entityType cannot be null");
        
        // Normalize entity type to uppercase for consistency
        entityType = entityType.toUpperCase();
    }

    /**
     * Creates a new NER condition without variable binding.
     * Example: NER("ORGANIZATION")
     */
    public static Ner of(String entityType) {
        return new Ner(entityType, null);
    }

    /**
     * Creates a new NER condition with variable binding.
     * Example: NER("PERSON", "?scientist")
     * 
     * @param entityType The type of entity to match (PERSON, ORGANIZATION, etc.)
     * @param variable The variable name (with ? prefix) to bind matches to
     */
    public static Ner withVariable(String entityType, String variable) {
        if (!variable.startsWith("?")) {
            throw new IllegalArgumentException("Variable name must start with ?");
        }
        return new Ner(entityType, variable.substring(1));
    }

    /**
     * Returns whether this condition uses variable binding.
     */
    public boolean isVariable() {
        return variableName != null;
    }
    
    @Override
    public String getType() {
        return "NER";
    }
    
    @Override
    public String toString() {
        if (isVariable()) {
            return String.format("NER(%s, ?%s)", entityType, variableName);
        }
        return String.format("NER(%s)", entityType);
    }
} 
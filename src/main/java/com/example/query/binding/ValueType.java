package com.example.query.binding;

/**
 * Defines the type of value captured in a MatchDetail.
 */
public enum ValueType {
    TERM,       // A simple text term
    DATE,       // A recognized date/time value
    ENTITY,     // A named entity (e.g., PERSON, ORGANIZATION)
    DEPENDENCY, // A grammatical dependency relation
    POS_TERM,   // A term with its part-of-speech tag
    // Add other types as needed
} 
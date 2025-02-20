package com.example.query.model.column;

/**
 * Represents the different types of columns that can be extracted from index matches.
 */
public enum ColumnType {
    /** Person names extracted from matches */
    PERSON,
    
    /** Temporal values extracted from matches */
    DATE,
    
    /** Place names extracted from matches */
    LOCATION,
    
    /** Matched terms from the query */
    TERM,
    
    /** Syntactic relations between terms */
    RELATION,
    
    /** Hypernym categories for terms */
    CATEGORY,
    
    /** Text context around matches */
    SNIPPET,
    
    /** Match frequency counts */
    COUNT
} 
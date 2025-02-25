package com.example.query.model;

/**
 * Represents a METADATA expression in the SELECT clause of a query.
 * This expression selects metadata fields from matching documents.
 */
public record MetadataNode() implements SelectColumn {
    
    @Override
    public String toString() {
        return "METADATA";
    }
} 
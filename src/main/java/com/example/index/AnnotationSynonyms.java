package com.example.index;

import java.io.IOException;

/**
 * Abstract base class for managing synonym mappings between annotation values and integer IDs.
 * Provides a mechanism to convert between strings and more compact integer representations
 * for efficient storage and retrieval.
 */
public abstract class AnnotationSynonyms implements AutoCloseable {
    /**
     * Gets or creates an ID for the given annotation value and type.
     * 
     * @param value The annotation value (e.g., a date, NER tag, POS tag, etc.)
     * @param type The type of annotation
     * @return The integer ID associated with this value
     * @throws IllegalArgumentException if the value format is invalid for the given type
     * @throws IllegalStateException if the synonyms database is closed
     */
    public abstract int getOrCreateId(String value, AnnotationType type);
    
    /**
     * Gets the annotation value for a given synonym ID and type.
     * 
     * @param id The ID to look up
     * @param type The type of annotation
     * @return The annotation value, or null if not found
     * @throws IllegalStateException if the synonyms database is closed
     */
    public abstract String getValue(int id, AnnotationType type);
    
    /**
     * Returns the number of annotation synonyms for a specific type.
     * 
     * @param type The annotation type
     * @return The number of synonyms for that type
     */
    public abstract int size(AnnotationType type);
    
    /**
     * Returns the total number of annotation synonyms across all types.
     * 
     * @return The total number of synonyms
     */
    public abstract int size();
    
    /**
     * Validates the consistency of all synonym mappings.
     * 
     * @throws IOException if validation fails with an I/O error
     */
    public abstract void validateSynonyms() throws IOException;
    
    @Override
    public abstract void close() throws IOException;
} 
package com.example.query.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Tracks variable bindings captured during query execution.
 * Variables can be bound to different values in different documents.
 */
public class VariableBindings {
    // Map from document ID to a map of variable name to value
    private final Map<Integer, Map<String, String>> bindings;

    /**
     * Creates a new empty VariableBindings object.
     */
    public VariableBindings() {
        this.bindings = new HashMap<>();
    }

    /**
     * Adds a variable binding for a document.
     *
     * @param documentId The document ID
     * @param variableName The variable name
     * @param value The value to bind to the variable
     */
    public void addBinding(int documentId, String variableName, String value) {
        bindings.computeIfAbsent(documentId, k -> new HashMap<>())
                .put(variableName, value);
    }

    /**
     * Gets the value of a variable for a document.
     *
     * @param documentId The document ID
     * @param variableName The variable name
     * @return Optional containing the value if bound, empty otherwise
     */
    public Optional<String> getValue(int documentId, String variableName) {
        return Optional.ofNullable(bindings.getOrDefault(documentId, Collections.emptyMap())
                .get(variableName));
    }

    /**
     * Gets all variable bindings for a document.
     *
     * @param documentId The document ID
     * @return Map of variable name to value
     */
    public Map<String, String> getBindingsForDocument(int documentId) {
        return Collections.unmodifiableMap(
                bindings.getOrDefault(documentId, Collections.emptyMap()));
    }

    /**
     * Gets all document IDs that have variable bindings.
     *
     * @return Set of document IDs
     */
    public Set<Integer> getDocumentIds() {
        return Collections.unmodifiableSet(bindings.keySet());
    }

    /**
     * Merges another VariableBindings object into this one.
     * If there are conflicts, the values from the other object take precedence.
     *
     * @param other The other VariableBindings object
     */
    public void merge(VariableBindings other) {
        for (Map.Entry<Integer, Map<String, String>> entry : other.bindings.entrySet()) {
            int documentId = entry.getKey();
            Map<String, String> docBindings = entry.getValue();
            
            for (Map.Entry<String, String> binding : docBindings.entrySet()) {
                addBinding(documentId, binding.getKey(), binding.getValue());
            }
        }
    }

    /**
     * Clears all variable bindings.
     */
    public void clear() {
        bindings.clear();
    }

    @Override
    public String toString() {
        return "VariableBindings{" +
                "bindings=" + bindings +
                '}';
    }
} 
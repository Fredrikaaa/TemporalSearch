package com.example.query.executor;

import com.example.query.model.DocSentenceMatch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Tracks variable bindings captured during query execution.
 * Variables can be bound to different values in different documents and sentences.
 */
public class VariableBindings {
    // Map from document ID to a map of variable name to value
    private final Map<Integer, Map<String, String>> documentBindings;
    
    // Map from SentenceKey to a map of variable name to value
    private final Map<SentenceKey, Map<String, String>> sentenceBindings;

    /**
     * Creates a new empty VariableBindings object.
     */
    public VariableBindings() {
        this.documentBindings = new HashMap<>();
        this.sentenceBindings = new HashMap<>();
    }

    /**
     * Adds a variable binding for a document.
     *
     * @param documentId The document ID
     * @param variableName The variable name
     * @param value The value to bind to the variable
     */
    public void addBinding(int documentId, String variableName, String value) {
        documentBindings.computeIfAbsent(documentId, k -> new HashMap<>())
                .put(variableName, value);
    }
    
    /**
     * Adds a variable binding for a specific sentence.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param variableName The variable name
     * @param value The value to bind to the variable
     */
    public void addBinding(int documentId, int sentenceId, String variableName, String value) {
        SentenceKey key = new SentenceKey(documentId, sentenceId);
        sentenceBindings.computeIfAbsent(key, k -> new HashMap<>())
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
        return Optional.ofNullable(documentBindings.getOrDefault(documentId, Collections.emptyMap())
                .get(variableName));
    }
    
    /**
     * Gets the value of a variable for a specific sentence.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param variableName The variable name
     * @return Optional containing the value if bound, empty otherwise
     */
    public Optional<String> getValue(int documentId, int sentenceId, String variableName) {
        SentenceKey key = new SentenceKey(documentId, sentenceId);
        return Optional.ofNullable(sentenceBindings.getOrDefault(key, Collections.emptyMap())
                .get(variableName));
    }
    
    /**
     * Gets the value of a variable, checking both sentence and document level bindings.
     * Sentence level bindings take precedence over document level bindings.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param variableName The variable name
     * @return Optional containing the value if bound, empty otherwise
     */
    public Optional<String> getValueWithFallback(int documentId, int sentenceId, String variableName) {
        // First check sentence level
        Optional<String> sentenceValue = getValue(documentId, sentenceId, variableName);
        if (sentenceValue.isPresent()) {
            return sentenceValue;
        }
        
        // Fall back to document level
        return getValue(documentId, variableName);
    }

    /**
     * Gets all variable bindings for a document.
     *
     * @param documentId The document ID
     * @return Map of variable name to value
     */
    public Map<String, String> getBindingsForDocument(int documentId) {
        return Collections.unmodifiableMap(
                documentBindings.getOrDefault(documentId, Collections.emptyMap()));
    }
    
    /**
     * Gets all variable bindings for a specific sentence.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @return Map of variable name to value
     */
    public Map<String, String> getBindingsForSentence(int documentId, int sentenceId) {
        SentenceKey key = new SentenceKey(documentId, sentenceId);
        return Collections.unmodifiableMap(
                sentenceBindings.getOrDefault(key, Collections.emptyMap()));
    }
    
    /**
     * Gets all variable bindings for a specific sentence, including document-level bindings.
     * Sentence-level bindings take precedence over document-level bindings.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @return Map of variable name to value
     */
    public Map<String, String> getAllBindingsForSentence(int documentId, int sentenceId) {
        Map<String, String> result = new HashMap<>(getBindingsForDocument(documentId));
        result.putAll(getBindingsForSentence(documentId, sentenceId));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Gets all document IDs that have variable bindings.
     *
     * @return Set of document IDs
     */
    public Set<Integer> getDocumentIds() {
        return Collections.unmodifiableSet(documentBindings.keySet());
    }
    
    /**
     * Gets all sentence keys that have variable bindings.
     *
     * @return Set of sentence keys
     */
    public Set<SentenceKey> getSentenceKeys() {
        return Collections.unmodifiableSet(sentenceBindings.keySet());
    }

    /**
     * Merges another VariableBindings object into this one.
     * If there are conflicts, the values from the other object take precedence.
     *
     * @param other The other VariableBindings object
     */
    public void merge(VariableBindings other) {
        // Merge document bindings
        for (Map.Entry<Integer, Map<String, String>> entry : other.documentBindings.entrySet()) {
            int documentId = entry.getKey();
            Map<String, String> docBindings = entry.getValue();
            
            for (Map.Entry<String, String> binding : docBindings.entrySet()) {
                addBinding(documentId, binding.getKey(), binding.getValue());
            }
        }
        
        // Merge sentence bindings
        for (Map.Entry<SentenceKey, Map<String, String>> entry : other.sentenceBindings.entrySet()) {
            SentenceKey key = entry.getKey();
            Map<String, String> sentBindings = entry.getValue();
            
            for (Map.Entry<String, String> binding : sentBindings.entrySet()) {
                addBinding(key.documentId, key.sentenceId, binding.getKey(), binding.getValue());
            }
        }
    }

    /**
     * Clears all variable bindings.
     */
    public void clear() {
        documentBindings.clear();
        sentenceBindings.clear();
    }

    /**
     * Gets the token position for a variable in a specific match.
     * This extracts the position from the variable binding's value format: term@sentenceId:position
     *
     * @param variableName The variable name
     * @param match The document-sentence match
     * @return The token position, or -1 if not found
     */
    public int getTokenPosition(String variableName, DocSentenceMatch match) {
        // Remove leading ? from variable name if present
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        // Check for variable binding
        Optional<String> value = getValueWithFallback(match.getDocumentId(), match.getSentenceId(), variableName);
        if (value.isEmpty()) {
            return -1;
        }
        
        // Parse the position from the value format: term@sentenceId:position
        String valueStr = value.get();
        int atPos = valueStr.lastIndexOf('@');
        if (atPos == -1) {
            return -1;
        }
        
        int colonPos = valueStr.lastIndexOf(':');
        if (colonPos == -1 || colonPos < atPos) {
            return -1;
        }
        
        try {
            return Integer.parseInt(valueStr.substring(colonPos + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "VariableBindings{" +
                "documentBindings=" + documentBindings +
                ", sentenceBindings=" + sentenceBindings +
                '}';
    }
    
    /**
     * Helper class for sentence identification
     */
    public static class SentenceKey {
        private final int documentId;
        private final int sentenceId;

        public SentenceKey(int documentId, int sentenceId) {
            this.documentId = documentId;
            this.sentenceId = sentenceId;
        }
        
        public int getDocumentId() {
            return documentId;
        }
        
        public int getSentenceId() {
            return sentenceId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SentenceKey that = (SentenceKey) o;
            return documentId == that.documentId && sentenceId == that.sentenceId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(documentId, sentenceId);
        }
        
        @Override
        public String toString() {
            return "SentenceKey{" +
                    "documentId=" + documentId +
                    ", sentenceId=" + sentenceId +
                    '}';
        }
    }
} 
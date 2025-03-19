package com.example.query.executor;

import com.example.query.model.DocSentenceMatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Tracks variable bindings captured during query execution.
 * Variables can be bound to multiple values in different documents and sentences.
 */
public class VariableBindings {
    // Map from document ID to a map of variable name to list of values
    private final Map<Integer, Map<String, List<String>>> documentBindings;
    
    // Map from SentenceKey to a map of variable name to list of values
    private final Map<SentenceKey, Map<String, List<String>>> sentenceBindings;

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
        Map<String, List<String>> docVars = documentBindings.computeIfAbsent(documentId, k -> new HashMap<>());
        List<String> values = docVars.computeIfAbsent(variableName, k -> new ArrayList<>());
        values.add(value);
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
        Map<String, List<String>> sentVars = sentenceBindings.computeIfAbsent(key, k -> new HashMap<>());
        List<String> values = sentVars.computeIfAbsent(variableName, k -> new ArrayList<>());
        values.add(value);
    }

    /**
     * Gets all values of a variable for a document.
     *
     * @param documentId The document ID
     * @param variableName The variable name
     * @return List of values (empty if no bindings)
     */
    public List<String> getValues(int documentId, String variableName) {
        Map<String, List<String>> docVars = documentBindings.getOrDefault(documentId, Collections.emptyMap());
        return new ArrayList<>(docVars.getOrDefault(variableName, Collections.emptyList()));
    }
    
    /**
     * Gets all values of a variable for a specific sentence.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param variableName The variable name
     * @return List of values (empty if no bindings)
     */
    public List<String> getValues(int documentId, int sentenceId, String variableName) {
        SentenceKey key = new SentenceKey(documentId, sentenceId);
        Map<String, List<String>> sentVars = sentenceBindings.getOrDefault(key, Collections.emptyMap());
        return new ArrayList<>(sentVars.getOrDefault(variableName, Collections.emptyList()));
    }
    
    /**
     * Gets the first value of a variable, checking both sentence and document level bindings.
     * Sentence level bindings take precedence over document level bindings.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param variableName The variable name
     * @return Optional containing the first value if bound, empty otherwise
     */
    public Optional<String> getValueWithFallback(int documentId, int sentenceId, String variableName) {
        // First check sentence level
        List<String> sentValues = getValues(documentId, sentenceId, variableName);
        if (!sentValues.isEmpty()) {
            return Optional.of(sentValues.get(0));
        }
        
        // Fall back to document level
        List<String> docValues = getValues(documentId, variableName);
        return docValues.isEmpty() ? Optional.empty() : Optional.of(docValues.get(0));
    }

    /**
     * Gets all variable values for a document.
     *
     * @param documentId The document ID
     * @return Map of variable name to list of values
     */
    public Map<String, List<String>> getValuesForDocument(int documentId) {
        Map<String, List<String>> result = new HashMap<>();
        Map<String, List<String>> docVars = documentBindings.getOrDefault(documentId, Collections.emptyMap());
        
        for (Map.Entry<String, List<String>> entry : docVars.entrySet()) {
            result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        
        return Collections.unmodifiableMap(result);
    }
    
    /**
     * Gets all variable values for a specific sentence.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @return Map of variable name to list of values
     */
    public Map<String, List<String>> getValuesForSentence(int documentId, int sentenceId) {
        Map<String, List<String>> result = new HashMap<>();
        SentenceKey key = new SentenceKey(documentId, sentenceId);
        Map<String, List<String>> sentVars = sentenceBindings.getOrDefault(key, Collections.emptyMap());
        
        for (Map.Entry<String, List<String>> entry : sentVars.entrySet()) {
            result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        
        return Collections.unmodifiableMap(result);
    }
    
    /**
     * Gets all variable values for a specific sentence, including document-level values.
     * This combines document and sentence level bindings.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @return Map of variable name to list of values
     */
    public Map<String, List<String>> getAllValuesForSentence(int documentId, int sentenceId) {
        Map<String, List<String>> result = new HashMap<>();
        
        // Add document level bindings
        Map<String, List<String>> docBindings = getValuesForDocument(documentId);
        for (Map.Entry<String, List<String>> entry : docBindings.entrySet()) {
            result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        
        // Add sentence level bindings
        Map<String, List<String>> sentBindings = getValuesForSentence(documentId, sentenceId);
        for (Map.Entry<String, List<String>> entry : sentBindings.entrySet()) {
            String varName = entry.getKey();
            List<String> values = entry.getValue();
            
            if (result.containsKey(varName)) {
                // Append to existing list
                result.get(varName).addAll(values);
            } else {
                // Create new list
                result.put(varName, new ArrayList<>(values));
            }
        }
        
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
     * All values from both objects are preserved.
     *
     * @param other The other VariableBindings object
     */
    public void merge(VariableBindings other) {
        // Merge document bindings
        for (Map.Entry<Integer, Map<String, List<String>>> entry : other.documentBindings.entrySet()) {
            int documentId = entry.getKey();
            Map<String, List<String>> docBindings = entry.getValue();
            
            for (Map.Entry<String, List<String>> binding : docBindings.entrySet()) {
                String varName = binding.getKey();
                List<String> values = binding.getValue();
                
                for (String value : values) {
                    addBinding(documentId, varName, value);
                }
            }
        }
        
        // Merge sentence bindings
        for (Map.Entry<SentenceKey, Map<String, List<String>>> entry : other.sentenceBindings.entrySet()) {
            SentenceKey key = entry.getKey();
            Map<String, List<String>> sentBindings = entry.getValue();
            
            for (Map.Entry<String, List<String>> binding : sentBindings.entrySet()) {
                String varName = binding.getKey();
                List<String> values = binding.getValue();
                
                for (String value : values) {
                    addBinding(key.documentId, key.sentenceId, varName, value);
                }
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
     * Gets the begin character position for a variable in a specific match.
     * This extracts the begin position from the variable binding's value format: term@beginPos:endPos
     *
     * @param variableName The variable name
     * @param match The document-sentence match
     * @return The begin character position, or -1 if not found
     */
    public int getBeginCharPosition(String variableName, DocSentenceMatch match) {
        // Remove leading ? from variable name if present
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        // Get the first value for this variable (for backward compatibility)
        Optional<String> valueOpt = getValueWithFallback(match.documentId(), match.sentenceId(), variableName);
        if (valueOpt.isEmpty()) {
            return -1;
        }
        
        // Parse the position from the value format: term@beginPos:endPos
        String valueStr = valueOpt.get();
        int atPos = valueStr.lastIndexOf('@');
        if (atPos == -1) {
            return -1;
        }
        
        int colonPos = valueStr.lastIndexOf(':');
        if (colonPos == -1 || colonPos < atPos) {
            return -1;
        }
        
        try {
            return Integer.parseInt(valueStr.substring(atPos + 1, colonPos));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * Gets the end character position for a variable in a specific match.
     * This extracts the end position from the variable binding's value format: term@beginPos:endPos
     *
     * @param variableName The variable name
     * @param match The document-sentence match
     * @return The end character position, or -1 if not found
     */
    public int getEndCharPosition(String variableName, DocSentenceMatch match) {
        // Remove leading ? from variable name if present
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        // Get the first value for this variable (for backward compatibility)
        Optional<String> valueOpt = getValueWithFallback(match.documentId(), match.sentenceId(), variableName);
        if (valueOpt.isEmpty()) {
            return -1;
        }
        
        // Parse the position from the value format: term@beginPos:endPos
        String valueStr = valueOpt.get();
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

    /**
     * Gets debugging information for a variable binding.
     *
     * @param variableName The variable name
     * @param match The document-sentence match
     * @return The debug information string, or empty string if not available
     */
    public String getVariableDebugInfo(String variableName, DocSentenceMatch match) {
        // Remove leading ? from variable name if present
        if (variableName.startsWith("?")) {
            variableName = variableName.substring(1);
        }
        
        // Get the first value for this variable
        Optional<String> valueOpt = getValueWithFallback(match.documentId(), match.sentenceId(), variableName);
        if (valueOpt.isEmpty()) {
            return "";
        }
        
        return valueOpt.get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VariableBindings Summary:\n");
        
        // Document bindings summary
        int docBindingsCount = documentBindings.size();
        sb.append("Document bindings: ").append(docBindingsCount).append(" documents\n");
        
        // Only show first few document bindings if there are many
        int docBindingsToShow = Math.min(docBindingsCount, 3);
        int docBindingsShown = 0;
        
        if (docBindingsCount > 0) {
            sb.append("Sample document bindings:\n");
            for (Map.Entry<Integer, Map<String, List<String>>> entry : documentBindings.entrySet()) {
                if (docBindingsShown >= docBindingsToShow) break;
                
                sb.append("  Doc ").append(entry.getKey()).append(": ");
                
                // Count variables for this document
                int varCount = entry.getValue().size();
                sb.append(varCount).append(" variables");
                
                // Show first few variables if any exist
                if (varCount > 0) {
                    sb.append(" (");
                    int varsShown = 0;
                    for (Map.Entry<String, List<String>> varEntry : entry.getValue().entrySet()) {
                        if (varsShown > 0) sb.append(", ");
                        if (varsShown >= 2) {
                            sb.append("...");
                            break;
                        }
                        
                        // Show variable name and count of values
                        sb.append(varEntry.getKey()).append(": ")
                          .append(varEntry.getValue().size()).append(" values");
                        
                        varsShown++;
                    }
                    sb.append(")");
                }
                
                sb.append("\n");
                docBindingsShown++;
            }
            
            if (docBindingsCount > docBindingsToShow) {
                sb.append("  ... and ").append(docBindingsCount - docBindingsToShow)
                  .append(" more documents\n");
            }
        }
        
        // Sentence bindings summary
        int sentKeyCount = sentenceBindings.size();
        sb.append("Sentence bindings: ").append(sentKeyCount).append(" sentences\n");
        
        // Only show first few sentence bindings if there are many
        int sentBindingsToShow = Math.min(sentKeyCount, 3);
        int sentBindingsShown = 0;
        
        if (sentKeyCount > 0) {
            sb.append("Sample sentence bindings:\n");
            for (Map.Entry<SentenceKey, Map<String, List<String>>> entry : sentenceBindings.entrySet()) {
                if (sentBindingsShown >= sentBindingsToShow) break;
                
                SentenceKey key = entry.getKey();
                sb.append("  Doc ").append(key.documentId)
                  .append(", Sent ").append(key.sentenceId).append(": ");
                
                // Count variables for this sentence
                int varCount = entry.getValue().size();
                sb.append(varCount).append(" variables");
                
                // Show first few variables if any exist
                if (varCount > 0) {
                    sb.append(" (");
                    int varsShown = 0;
                    for (Map.Entry<String, List<String>> varEntry : entry.getValue().entrySet()) {
                        if (varsShown > 0) sb.append(", ");
                        if (varsShown >= 2) {
                            sb.append("...");
                            break;
                        }
                        
                        // Show variable name and first value (truncated if needed)
                        String varName = varEntry.getKey();
                        List<String> values = varEntry.getValue();
                        
                        sb.append(varName).append(": ");
                        if (values.isEmpty()) {
                            sb.append("no values");
                        } else {
                            String value = values.get(0);
                            if (value.length() > 20) {
                                value = value.substring(0, 17) + "...";
                            }
                            sb.append("\"").append(value).append("\"");
                            if (values.size() > 1) {
                                sb.append(" +").append(values.size() - 1).append(" more");
                            }
                        }
                        
                        varsShown++;
                    }
                    sb.append(")");
                }
                
                sb.append("\n");
                sentBindingsShown++;
            }
            
            if (sentKeyCount > sentBindingsToShow) {
                sb.append("  ... and ").append(sentKeyCount - sentBindingsToShow)
                  .append(" more sentences\n");
            }
        }
        
        return sb.toString();
    }

    /**
     * Key class for sentence-level bindings.
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
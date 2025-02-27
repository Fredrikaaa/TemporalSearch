package com.example.query.model;

import com.example.core.Position;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents a match at either document or sentence level.
 * This is the core data structure for representing matches with different granularity.
 */
public class DocSentenceMatch {
    private final int documentId;
    private final int sentenceId;  // -1 for document-level matches
    private final Map<String, Set<Position>> matchPositions;

    /**
     * Constructor for sentence-level match.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     */
    public DocSentenceMatch(int documentId, int sentenceId) {
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.matchPositions = new HashMap<>();
    }

    /**
     * Constructor for document-level match.
     *
     * @param documentId The document ID
     */
    public DocSentenceMatch(int documentId) {
        this(documentId, -1);
    }

    /**
     * Adds a position to the match for a specific key.
     *
     * @param key The key (e.g., variable name or condition identifier)
     * @param position The position to add
     */
    public void addPosition(String key, Position position) {
        matchPositions.computeIfAbsent(key, k -> new HashSet<>()).add(position);
    }

    /**
     * Adds multiple positions to the match for a specific key.
     *
     * @param key The key (e.g., variable name or condition identifier)
     * @param positions The positions to add
     */
    public void addPositions(String key, Set<Position> positions) {
        if (positions == null || positions.isEmpty()) {
            return;
        }
        matchPositions.computeIfAbsent(key, k -> new HashSet<>()).addAll(positions);
    }

    /**
     * Gets the document ID.
     *
     * @return The document ID
     */
    public int getDocumentId() {
        return documentId;
    }

    /**
     * Gets the sentence ID.
     *
     * @return The sentence ID, or -1 for document-level matches
     */
    public int getSentenceId() {
        return sentenceId;
    }

    /**
     * Checks if this is a sentence-level match.
     *
     * @return true if this is a sentence-level match, false if document-level
     */
    public boolean isSentenceLevel() {
        return sentenceId >= 0;
    }

    /**
     * Gets all positions for a specific key.
     *
     * @param key The key to get positions for
     * @return Set of positions for the key, or empty set if none
     */
    public Set<Position> getPositions(String key) {
        return matchPositions.getOrDefault(key, Collections.emptySet());
    }

    /**
     * Gets all match positions.
     *
     * @return Map of key to positions
     */
    public Map<String, Set<Position>> getAllPositions() {
        return Collections.unmodifiableMap(matchPositions);
    }

    /**
     * Gets all keys with positions.
     *
     * @return Set of keys
     */
    public Set<String> getKeys() {
        return matchPositions.keySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocSentenceMatch that = (DocSentenceMatch) o;
        return documentId == that.documentId && sentenceId == that.sentenceId;
    }

    @Override
    public int hashCode() {
        int result = documentId;
        result = 31 * result + sentenceId;
        return result;
    }

    @Override
    public String toString() {
        return "DocSentenceMatch{" +
                "documentId=" + documentId +
                ", sentenceId=" + sentenceId +
                ", keys=" + matchPositions.keySet() +
                ", positionCount=" + matchPositions.values().stream().mapToInt(Set::size).sum() +
                '}';
    }
} 
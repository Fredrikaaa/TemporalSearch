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
public record DocSentenceMatch(
    int documentId,
    int sentenceId,  // -1 for document-level matches
    Map<String, Set<Position>> matchPositions
) {
    /**
     * Constructor for sentence-level match.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     */
    public DocSentenceMatch(int documentId, int sentenceId) {
        this(documentId, sentenceId, new HashMap<>());
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
     * Compact constructor to ensure defensive copy of matchPositions.
     */
    public DocSentenceMatch {
        matchPositions = matchPositions != null ? new HashMap<>(matchPositions) : new HashMap<>();
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

    public void mergePositions(DocSentenceMatch other) {
        other.matchPositions.forEach((key, positions) -> 
            this.matchPositions.merge(key, positions, (a, b) -> {
                a.addAll(b);
                return a;
            })
        );
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
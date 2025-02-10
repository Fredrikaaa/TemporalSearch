package com.example.core;

import java.util.Objects;
import java.time.LocalDate;

/**
 * Represents the exact location of a word, phrase, or entity within the document collection. 
 * Each Position instance stores a document ID, sentence ID, character-level begin and end positions, 
 * and a timestamp. This immutable class serves as the fundamental building block for all index 
 * operations, enabling precise text location tracking and temporal analysis. It includes proper 
 * equality checking and comparison methods to support set operations and sorting.
 */
public class Position {
    private final int documentId;
    private final int sentenceId;
    private final int beginPosition;
    private final int endPosition;
    private final LocalDate timestamp;

    public Position(int documentId, int sentenceId, int beginPosition, int endPosition, LocalDate timestamp) {
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.beginPosition = beginPosition;
        this.endPosition = endPosition;
        this.timestamp = timestamp;
    }

    // Getters
    public int getDocumentId() {
        return documentId;
    }

    public int getSentenceId() {
        return sentenceId;
    }

    public int getBeginPosition() {
        return beginPosition;
    }

    public int getEndPosition() {
        return endPosition;
    }

    public LocalDate getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("Position(doc=%d, sent=%d, begin=%d, end=%d, time=%s)",
                documentId, sentenceId, beginPosition, endPosition, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Position position = (Position) o;
        return documentId == position.documentId &&
                sentenceId == position.sentenceId &&
                beginPosition == position.beginPosition &&
                endPosition == position.endPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(documentId, sentenceId, beginPosition, endPosition);
    }
}

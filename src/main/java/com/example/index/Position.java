package com.example.index;

import java.time.LocalDate;

/**
 * Represents a position of a word or phrase in the document.
 * Immutable class to ensure thread safety.
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
        if (!(o instanceof Position))
            return false;
        Position other = (Position) o;
        return documentId == other.documentId &&
                sentenceId == other.sentenceId &&
                beginPosition == other.beginPosition &&
                endPosition == other.endPosition &&
                timestamp.equals(other.timestamp);
    }

    @Override
    public int hashCode() {
        int result = documentId;
        result = 31 * result + sentenceId;
        result = 31 * result + beginPosition;
        result = 31 * result + endPosition;
        result = 31 * result + timestamp.hashCode();
        return result;
    }
}

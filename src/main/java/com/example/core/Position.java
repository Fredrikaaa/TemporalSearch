package com.example.core;

import java.time.LocalDate;

/**
 * Represents a position in the corpus, including document location and temporal information.
 * This class is used by both index generation and query components.
 */
public class Position implements Comparable<Position> {
    private final int documentId;
    private final int sentenceId;
    private final int beginChar;
    private final int endChar;
    private final LocalDate timestamp;

    public Position(int documentId, int sentenceId, int beginChar, int endChar, LocalDate timestamp) {
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.beginChar = beginChar;
        this.endChar = endChar;
        this.timestamp = timestamp;
    }

    public int getDocumentId() {
        return documentId;
    }

    public int getSentenceId() {
        return sentenceId;
    }

    public int getBeginChar() {
        return beginChar;
    }

    public int getEndChar() {
        return endChar;
    }

    public LocalDate getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(Position other) {
        int docCompare = Integer.compare(this.documentId, other.documentId);
        if (docCompare != 0) return docCompare;
        
        int sentCompare = Integer.compare(this.sentenceId, other.sentenceId);
        if (sentCompare != 0) return sentCompare;
        
        return Integer.compare(this.beginChar, other.beginChar);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Position other = (Position) o;
        return documentId == other.documentId &&
               sentenceId == other.sentenceId &&
               beginChar == other.beginChar &&
               endChar == other.endChar &&
               timestamp.equals(other.timestamp);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + documentId;
        result = 31 * result + sentenceId;
        result = 31 * result + beginChar;
        result = 31 * result + endChar;
        result = 31 * result + timestamp.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("Position(doc=%d, sent=%d, span=[%d,%d], time=%s)",
            documentId, sentenceId, beginChar, endChar, timestamp);
    }
} 
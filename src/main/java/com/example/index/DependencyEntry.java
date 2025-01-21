package com.example.index;

import java.time.LocalDate;

/**
 * Represents a dependency relation entry from the SQLite database.
 * Contains information about the head token, dependent token, and their relation.
 */
public final class DependencyEntry implements IndexEntry {
    private final int documentId;
    private final int sentenceId;
    private final int beginChar;
    private final int endChar;
    private final String headToken;
    private final String dependentToken;
    private final String relation;
    private final LocalDate timestamp;

    public DependencyEntry(int documentId, int sentenceId, int beginChar, int endChar,
            String headToken, String dependentToken, String relation, LocalDate timestamp) {
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.beginChar = beginChar;
        this.endChar = endChar;
        this.headToken = headToken;
        this.dependentToken = dependentToken;
        this.relation = relation;
        this.timestamp = timestamp;
    }

    @Override
    public int getDocumentId() {
        return documentId;
    }

    @Override
    public int getSentenceId() {
        return sentenceId;
    }

    @Override
    public int getBeginChar() {
        return beginChar;
    }

    @Override
    public int getEndChar() {
        return endChar;
    }

    @Override
    public LocalDate getTimestamp() {
        return timestamp;
    }

    /**
     * @return The head token in the dependency relation
     */
    public String getHeadToken() {
        return headToken;
    }

    /**
     * @return The dependent token in the dependency relation
     */
    public String getDependentToken() {
        return dependentToken;
    }

    /**
     * @return The type of dependency relation
     */
    public String getRelation() {
        return relation;
    }
} 
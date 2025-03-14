package com.example.index;

import java.time.LocalDate;

/**
 * Represents an annotation entry from the SQLite database.
 * Contains information about a token's lemma and part-of-speech tag.
 */
public final class AnnotationEntry implements IndexEntry {
    private final int annotationId;
    private final int documentId;
    private final int sentenceId;
    private final int beginChar;
    private final int endChar;
    private final String lemma;
    private final String pos;
    private final LocalDate timestamp;

    public AnnotationEntry(int annotationId, int documentId, int sentenceId, int beginChar, int endChar,
            String lemma, String pos, LocalDate timestamp) {
        this.annotationId = annotationId;
        this.documentId = documentId;
        this.sentenceId = sentenceId;
        this.beginChar = beginChar;
        this.endChar = endChar;
        this.lemma = lemma;
        this.pos = pos;
        this.timestamp = timestamp;
    }

    /**
     * @return The unique identifier for this annotation
     */
    public int getAnnotationId() {
        return annotationId;
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
     * @return The lemmatized form of the token
     */
    public String getLemma() {
        return lemma;
    }

    /**
     * @return The part-of-speech tag for the token
     */
    public String getPos() {
        return pos;
    }
} 
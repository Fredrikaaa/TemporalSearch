package com.example.index;

import java.time.LocalDate;

/**
 * Represents an entry in the stitch index, containing either an ngram or a date value.
 * For ngrams, synonymId will be -1. For dates, it will be a valid ID from the date_synonyms table.
 */
public record StitchEntry(
    int documentId,
    int sentenceId,
    int beginChar,
    int endChar,
    LocalDate timestamp,
    String value,        // The actual text (ngram or YYYY-MM-DD date)
    int synonymId        // -1 for ngrams, valid ID for dates from date_synonyms table
) implements IndexEntry {
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
} 
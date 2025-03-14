package com.example.index;

import com.example.core.Position;
import java.time.LocalDate;

/**
 * Represents a position in the stitch index, containing either an ngram or date position.
 * For ngrams, synonymId will be -1. For dates, it will be a valid ID from the date_synonyms table.
 */
public class StitchPosition extends Position {
    private final int synonymId;  // -1 for ngrams, valid ID for dates
    
    // Type identifier for serialization
    public static final byte POSITION_TYPE = 1;

    public StitchPosition(
        int documentId,
        int sentenceId,
        int beginPosition,
        int endPosition,
        LocalDate timestamp,
        int synonymId        // -1 for ngrams, valid ID for dates
    ) {
        super(documentId, sentenceId, beginPosition, endPosition, timestamp);
        this.synonymId = synonymId;
    }

    public int getSynonymId() {
        return synonymId;
    }
    
    /**
     * Creates a StitchPosition from a regular Position by adding the synonym ID.
     * 
     * @param position The base position
     * @param synonymId The date synonym ID
     * @return A new StitchPosition
     */
    public static StitchPosition fromPosition(Position position, int synonymId) {
        return new StitchPosition(
            position.getDocumentId(),
            position.getSentenceId(),
            position.getBeginPosition(),
            position.getEndPosition(),
            position.getTimestamp(),
            synonymId
        );
    }
    
    @Override
    public String toString() {
        return String.format("StitchPosition(doc=%d, sent=%d, begin=%d, end=%d, time=%s, synonymId=%d)",
                getDocumentId(), getSentenceId(), getBeginPosition(), getEndPosition(), 
                getTimestamp(), synonymId);
    }
} 
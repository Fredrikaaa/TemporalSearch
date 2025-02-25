package com.example.query.model.document;

/**
 * Represents the start and end boundaries of a text segment.
 * This could be a document, sentence, or any other text span.
 */
public record TextBoundary(int start, int end) {
    /**
     * Creates a new TextBoundary with the given start and end positions.
     * 
     * @param start The start position (inclusive)
     * @param end The end position (exclusive)
     */
    public TextBoundary {
        if (end < start) {
            throw new IllegalArgumentException("End position cannot be before start position");
        }
    }
    
    /**
     * Returns the length of this text boundary.
     * 
     * @return The number of characters in this text span
     */
    public int length() {
        return end - start;
    }
    
    /**
     * Checks if this boundary contains the given position.
     * 
     * @param position The position to check
     * @return true if the position is within this boundary, false otherwise
     */
    public boolean contains(int position) {
        return position >= start && position < end;
    }
    
    /**
     * Checks if this boundary overlaps with another boundary.
     * 
     * @param other The other boundary to check
     * @return true if the boundaries overlap, false otherwise
     */
    public boolean overlaps(TextBoundary other) {
        return !(end <= other.start || start >= other.end);
    }
    
    @Override
    public String toString() {
        return "[" + start + ", " + end + ")";
    }
} 
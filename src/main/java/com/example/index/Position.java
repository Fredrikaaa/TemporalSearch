package com.example.index;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents a position in a document with associated metadata.
 */
public record Position(
    int docId,
    int start,
    int end,
    LocalDateTime timestamp
) {
    /**
     * Creates a new position with validation.
     */
    public Position {
        if (docId < 0) {
            throw new IllegalArgumentException("Document ID must be non-negative");
        }
        if (start < 0) {
            throw new IllegalArgumentException("Start position must be non-negative");
        }
        if (end < start) {
            throw new IllegalArgumentException("End position must be greater than or equal to start position");
        }
        Objects.requireNonNull(timestamp, "Timestamp cannot be null");
    }

    /**
     * Creates a position without a timestamp.
     * @param docId The document ID
     * @param start The start position
     * @param end The end position
     * @return A new Position with the current timestamp
     */
    public static Position of(int docId, int start, int end) {
        return new Position(docId, start, end, LocalDateTime.now());
    }

    /**
     * Gets the length of this position.
     * @return The number of characters between start and end
     */
    public int length() {
        return end - start;
    }

    /**
     * Checks if this position overlaps with another position.
     * @param other The other position to check
     * @return true if the positions overlap
     */
    public boolean overlaps(Position other) {
        return this.docId == other.docId &&
            this.start < other.end &&
            this.end > other.start;
    }

    /**
     * Checks if this position contains another position.
     * @param other The other position to check
     * @return true if this position fully contains the other position
     */
    public boolean contains(Position other) {
        return this.docId == other.docId &&
            this.start <= other.start &&
            this.end >= other.end;
    }

    /**
     * Creates a new position that spans both this position and another position.
     * @param other The other position to merge with
     * @return A new Position that covers both positions
     * @throws IllegalArgumentException if the positions are not in the same document
     */
    public Position merge(Position other) {
        if (this.docId != other.docId) {
            throw new IllegalArgumentException("Cannot merge positions from different documents");
        }
        return new Position(
            this.docId,
            Math.min(this.start, other.start),
            Math.max(this.end, other.end),
            this.timestamp.isAfter(other.timestamp) ? this.timestamp : other.timestamp
        );
    }
} 
package com.example.index;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Manages a list of positions and handles serialization/deserialization.
 * Uses compression for efficient storage in LevelDB.
 */
public class PositionList {
    private final List<Position> positions;
    private static final int COMPRESSION_BUFFER_SIZE = 1024 * 1024; // 1MB

    public PositionList() {
        this.positions = new ArrayList<>();
    }

    public void add(Position position) {
        positions.add(position);
    }

    public List<Position> getPositions() {
        return Collections.unmodifiableList(positions);
    }

    /**
     * Serializes the position list to a compressed byte array.
     */
    public byte[] serialize() {
        // First serialize to uncompressed format
        byte[] uncompressed = serializeUncompressed();

        // Then compress
        Deflater deflater = new Deflater();
        deflater.setInput(uncompressed);
        deflater.finish();

        byte[] compressed = new byte[COMPRESSION_BUFFER_SIZE];
        int compressedLength = deflater.deflate(compressed);
        deflater.end();

        // Create final array of exact size
        byte[] result = new byte[compressedLength];
        System.arraycopy(compressed, 0, result, 0, compressedLength);

        return result;
    }

    private byte[] serializeUncompressed() {
        int size = positions.size();
        ByteBuffer buffer = ByteBuffer.allocate(calculateBufferSize(size));

        // Write size first
        buffer.putInt(size);

        // Write each position
        for (Position pos : positions) {
            buffer.putInt(pos.getDocumentId());
            buffer.putInt(pos.getSentenceId());
            buffer.putInt(pos.getBeginPosition());
            buffer.putInt(pos.getEndPosition());
            buffer.putLong(pos.getTimestamp().toEpochDay());
        }

        return buffer.array();
    }

    private int calculateBufferSize(int numPositions) {
        // 4 bytes for size
        // For each position:
        // - 4 bytes each for documentId, sentenceId, beginPosition, endPosition
        // - 8 bytes for timestamp
        return 4 + (numPositions * (4 * 4 + 8));
    }

    /**
     * Deserializes a byte array back into a PositionList.
     */
    public static PositionList deserialize(byte[] compressed) {
        try {
            // Decompress
            Inflater inflater = new Inflater();
            inflater.setInput(compressed);

            byte[] uncompressed = new byte[COMPRESSION_BUFFER_SIZE];
            int uncompressedLength = inflater.inflate(uncompressed);
            inflater.end();

            // Create buffer from uncompressed data
            ByteBuffer buffer = ByteBuffer.wrap(uncompressed, 0, uncompressedLength);

            // Read size
            int size = buffer.getInt();
            PositionList list = new PositionList();

            // Read each position
            for (int i = 0; i < size; i++) {
                int documentId = buffer.getInt();
                int sentenceId = buffer.getInt();
                int beginPosition = buffer.getInt();
                int endPosition = buffer.getInt();
                LocalDate timestamp = LocalDate.ofEpochDay(buffer.getLong());

                list.add(new Position(documentId, sentenceId, beginPosition, endPosition, timestamp));
            }

            return list;
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing positions", e);
        }
    }

    /**
     * Merges another PositionList into this one.
     */
    public void merge(PositionList other) {
        positions.addAll(other.positions);
    }

    /**
     * Returns the number of positions in the list.
     */
    public int size() {
        return positions.size();
    }

    /**
     * Sorts positions by document ID and then by position within document.
     */
    public void sort() {
        positions.sort((a, b) -> {
            int docCompare = Integer.compare(a.getDocumentId(), b.getDocumentId());
            if (docCompare != 0)
                return docCompare;

            int sentCompare = Integer.compare(a.getSentenceId(), b.getSentenceId());
            if (sentCompare != 0)
                return sentCompare;

            return Integer.compare(a.getBeginPosition(), b.getBeginPosition());
        });
    }
}

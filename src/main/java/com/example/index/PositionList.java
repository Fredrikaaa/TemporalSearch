package com.example.index;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Manages a list of positions with efficient storage using delta encoding.
 * Each position after the first is stored as a difference from the previous
 * position.
 */
public class PositionList {
    // Buffer size for compression operations
    private static final int COMPRESSION_BUFFER_SIZE = 1024 * 1024; // 1MB

    // Store positions in memory for fast access during processing
    private final List<Position> positions;

    // Cache serialized form to avoid recomputing unless needed
    private transient byte[] cachedBytes;
    private transient boolean isDirty;

    public PositionList() {
        this.positions = new ArrayList<>();
        this.isDirty = true;
    }

    public void add(Position position) {
        positions.add(position);
        isDirty = true;
    }

    public List<Position> getPositions() {
        return Collections.unmodifiableList(positions);
    }

    public int size() {
        return positions.size();
    }

    /**
     * Serializes positions using delta encoding for efficiency.
     * The first position is stored completely, subsequent positions store deltas.
     */
    public byte[] serialize() {
        if (!isDirty && cachedBytes != null) {
            return cachedBytes;
        }

        byte[] uncompressed = serializeToBytes();

        // Compress the serialized data
        Deflater deflater = new Deflater();
        deflater.setInput(uncompressed);
        deflater.finish();

        byte[] compressed = new byte[COMPRESSION_BUFFER_SIZE];
        int compressedLength = deflater.deflate(compressed);
        deflater.end();

        // Create final array of exact size
        cachedBytes = new byte[compressedLength];
        System.arraycopy(compressed, 0, cachedBytes, 0, compressedLength);
        isDirty = false;

        return cachedBytes;
    }

    private byte[] serializeToBytes() {
        // Calculate size needed: 4 bytes for count + space for each position
        int bufferSize = 4 + (positions.size() * 32); // Conservative estimate
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // Write number of positions
        buffer.putInt(positions.size());

        if (positions.isEmpty()) {
            return Arrays.copyOf(buffer.array(), buffer.position());
        }

        // Write first position completely
        Position first = positions.get(0);
        writeFullPosition(buffer, first);

        // Write subsequent positions as deltas
        for (int i = 1; i < positions.size(); i++) {
            Position prev = positions.get(i - 1);
            Position curr = positions.get(i);
            writeDeltaPosition(buffer, prev, curr);
        }

        // Create array of exact size used
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    private void writeFullPosition(ByteBuffer buffer, Position pos) {
        buffer.putInt(pos.getDocumentId());
        buffer.putInt(pos.getSentenceId());
        buffer.putInt(pos.getBeginPosition());
        buffer.putInt(pos.getEndPosition());
        buffer.putLong(pos.getTimestamp().toEpochDay());
    }

    private void writeDeltaPosition(ByteBuffer buffer, Position prev, Position curr) {
        // Store deltas using variable-length encoding
        writeVarInt(buffer, curr.getDocumentId() - prev.getDocumentId());
        writeVarInt(buffer, curr.getSentenceId() - prev.getSentenceId());
        writeVarInt(buffer, curr.getBeginPosition() - prev.getBeginPosition());
        writeVarInt(buffer, curr.getEndPosition() - prev.getEndPosition());
        writeVarInt(buffer, (int) (curr.getTimestamp().toEpochDay() - prev.getTimestamp().toEpochDay()));
    }

    /**
     * Deserializes and decompresses a byte array back into a PositionList.
     */
    public static PositionList deserialize(byte[] compressed) {
        // Decompress the data
        Inflater inflater = new Inflater();
        inflater.setInput(compressed);

        byte[] uncompressed = new byte[COMPRESSION_BUFFER_SIZE];
        int uncompressedLength;
        try {
            uncompressedLength = inflater.inflate(uncompressed);
        } catch (Exception e) {
            throw new RuntimeException("Error inflating compressed data", e);
        } finally {
            inflater.end();
        }

        // Create buffer from uncompressed data
        ByteBuffer buffer = ByteBuffer.wrap(uncompressed, 0, uncompressedLength);

        // Read number of positions
        int size = buffer.getInt();
        PositionList list = new PositionList();

        if (size == 0) {
            return list;
        }

        // Read first position
        Position first = readFullPosition(buffer);
        list.add(first);

        // Read deltas and reconstruct remaining positions
        Position prev = first;
        for (int i = 1; i < size; i++) {
            Position curr = readDeltaPosition(buffer, prev);
            list.add(curr);
            prev = curr;
        }

        return list;
    }

    private static Position readFullPosition(ByteBuffer buffer) {
        int docId = buffer.getInt();
        int sentId = buffer.getInt();
        int beginPos = buffer.getInt();
        int endPos = buffer.getInt();
        LocalDate timestamp = LocalDate.ofEpochDay(buffer.getLong());

        return new Position(docId, sentId, beginPos, endPos, timestamp);
    }

    private static Position readDeltaPosition(ByteBuffer buffer, Position prev) {
        int docId = prev.getDocumentId() + readVarInt(buffer);
        int sentId = prev.getSentenceId() + readVarInt(buffer);
        int beginPos = prev.getBeginPosition() + readVarInt(buffer);
        int endPos = prev.getEndPosition() + readVarInt(buffer);
        LocalDate timestamp = prev.getTimestamp().plusDays(readVarInt(buffer));

        return new Position(docId, sentId, beginPos, endPos, timestamp);
    }

    /**
     * Variable-length integer encoding.
     * Uses fewer bytes for smaller numbers.
     */
    private static void writeVarInt(ByteBuffer buffer, int value) {
        // Handle negative numbers with zigzag encoding
        value = (value << 1) ^ (value >> 31);
        while ((value & ~0x7F) != 0) {
            buffer.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    private static int readVarInt(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        byte b;

        do {
            b = buffer.get();
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);

        // Restore zigzag encoding
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Merges another PositionList into this one.
     */
    public void merge(PositionList other) {
        positions.addAll(other.positions);
        sort(); // Keep positions ordered
        isDirty = true;
    }

    /**
     * Sorts positions by document ID and position within document.
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
        isDirty = true;
    }
}

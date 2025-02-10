package com.example.core;

import me.lemire.integercompression.*;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.LogSampler;

/**
 * Manages collections of Position objects with efficient compression and serialization capabilities.
 * Uses FastPFOR128 compression to minimize storage requirements while maintaining quick access times.
 * Supports operations like merging, sorting, and deduplication of positions. The class handles
 * serialization by separating position data into parallel arrays for optimal compression ratios,
 * making it suitable for storage in key-value databases. Thread-safe for read operations through
 * unmodifiable list views.
 */
public class PositionList {
    private static final Logger logger = LoggerFactory.getLogger(PositionList.class);
    private static final LogSampler logSampler = new LogSampler(0.001);
    private final List<Position> positions;
    private static final IntegerCODEC codec = new FastPFOR128();
    
    public PositionList() {
        this.positions = new ArrayList<>();
    }

    public synchronized void add(Position position) {
        if (position == null) {
            logger.warn("Attempted to add null position");
            return;
        }
        positions.add(position);
        
        if (logSampler.shouldLog()) {
            logger.debug("Added position - docId: {}, sentenceId: {}, begin: {}, end: {}",
                position.getDocumentId(), position.getSentenceId(),
                position.getBeginPosition(), position.getEndPosition());
        }
    }

    public List<Position> getPositions() {
        return Collections.unmodifiableList(positions);
    }

    public byte[] serialize() {
        if (positions.isEmpty()) {
            logger.debug("Serializing empty position list");
            return new byte[0];
        }

        try {
            // Sort positions for efficient compression
            sort();

            // Prepare arrays for compression
            int[] docIds = new int[positions.size()];
            int[] sentenceIds = new int[positions.size()];
            int[] beginPositions = new int[positions.size()];
            int[] endPositions = new int[positions.size()];
            long[] timestamps = new long[positions.size()];
            
            // Store original values
            for (int i = 0; i < positions.size(); i++) {
                Position pos = positions.get(i);
                docIds[i] = pos.getDocumentId();
                sentenceIds[i] = pos.getSentenceId();
                beginPositions[i] = pos.getBeginPosition();
                endPositions[i] = pos.getEndPosition();
                timestamps[i] = pos.getTimestamp().toEpochDay();
            }

            if (logSampler.shouldLog()) {
                logger.debug("Serializing {} positions, first docId: {}, last docId: {}",
                    positions.size(), docIds[0], docIds[positions.size() - 1]);
            }

            // Allocate buffer with estimated size
            ByteBuffer buffer = ByteBuffer.allocate(positions.size() * 24 + 128);

            // Write metadata
            buffer.putInt(positions.size());

            // Compress each array individually with proper size checks
            for (int[] array : new int[][]{docIds, sentenceIds, beginPositions, endPositions}) {
                IntWrapper inOffset = new IntWrapper(0);
                IntWrapper outOffset = new IntWrapper(0);
                
                if (array.length <= 128) {  // Don't compress small arrays
                    buffer.putInt(-array.length);
                    for (int i = 0; i < array.length; i++) {
                        buffer.putInt(array[i]);
                    }
                    continue;
                }
                
                // Calculate number of complete blocks
                int blockSize = 128;
                int numBlocks = (array.length + blockSize - 1) / blockSize;
                int paddedSize = numBlocks * blockSize;
                
                // Create padded array
                int[] paddedArray = Arrays.copyOf(array, paddedSize);
                int[] compressed = new int[paddedSize * 2]; // Double size for safety
                
                // Try compression
                codec.compress(paddedArray, inOffset, paddedSize, compressed, outOffset);
                int compressedSize = outOffset.get();
                
                // Store the actual length and compressed size
                buffer.putInt(array.length);  // Original length
                buffer.putInt(compressedSize); // Compressed size
                for (int i = 0; i < compressedSize; i++) {
                    buffer.putInt(compressed[i]);
                }
            }

            // Write timestamps separately (not compressed)
            for (long timestamp : timestamps) {
                buffer.putLong(timestamp);
            }

            // Create exact-sized result
            byte[] result = new byte[buffer.position()];
            System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
            return result;
        } catch (Exception e) {
            logger.error("Failed to serialize position list: {}", e.getMessage(), e);
            throw e;
        }
    }

    public static PositionList deserialize(byte[] data) {
        if (data.length == 0) {
            logger.debug("Deserializing empty position list");
            return new PositionList();
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            PositionList result = new PositionList();

            // Read metadata
            int count = buffer.getInt();
            logger.debug("Deserializing {} positions", count);

            // Prepare arrays
            int[] docIds = new int[count];
            int[] sentenceIds = new int[count];
            int[] beginPositions = new int[count];
            int[] endPositions = new int[count];
            long[] timestamps = new long[count];

            IntWrapper inOffset = new IntWrapper(0);
            IntWrapper outOffset = new IntWrapper(0);

            // Read and decompress each array
            for (int[] array : new int[][]{docIds, sentenceIds, beginPositions, endPositions}) {
                int size = buffer.getInt();
                
                if (size < 0) {  // Uncompressed data
                    size = -size;
                    for (int i = 0; i < size; i++) {
                        array[i] = buffer.getInt();
                    }
                } else {
                    int originalLength = size;
                    int compressedSize = buffer.getInt();
                    
                    // Calculate padded size
                    int blockSize = 128;
                    int numBlocks = (originalLength + blockSize - 1) / blockSize;
                    int paddedSize = numBlocks * blockSize;
                    
                    int[] compressed = new int[compressedSize];
                    int[] decompressed = new int[paddedSize];
                    
                    for (int i = 0; i < compressedSize; i++) {
                        compressed[i] = buffer.getInt();
                    }
                    
                    inOffset.set(0);
                    outOffset.set(0);
                    
                    // Decompress to padded array
                    codec.uncompress(compressed, inOffset, compressedSize, decompressed, outOffset);
                    
                    // Copy only the needed values
                    System.arraycopy(decompressed, 0, array, 0, originalLength);
                }
            }

            // Read timestamps
            for (int i = 0; i < count; i++) {
                timestamps[i] = buffer.getLong();
            }

            // Create Position objects
            for (int i = 0; i < count; i++) {
                result.add(new Position(
                    docIds[i],
                    sentenceIds[i],
                    beginPositions[i],
                    endPositions[i],
                    LocalDate.ofEpochDay(timestamps[i])
                ));
            }

            return result;
        } catch (Exception e) {
            logger.error("Failed to deserialize position list: {}", e.getMessage(), e);
            throw e;
        }
    }

    public synchronized void merge(PositionList other) {
        if (other == null) {
            logger.warn("Attempted to merge with null PositionList");
            return;
        }

        int initialSize = positions.size();
        int otherSize = other.size();

        try {
            // Use TreeSet with custom comparator for ordered deduplication
            TreeSet<Position> positionSet = new TreeSet<>((a, b) -> {
                // First compare document IDs
                int docCompare = Integer.compare(a.getDocumentId(), b.getDocumentId());
                if (docCompare != 0) return docCompare;
                
                // Then compare sentence IDs
                int sentCompare = Integer.compare(a.getSentenceId(), b.getSentenceId());
                if (sentCompare != 0) return sentCompare;
                
                // For positions within the same sentence, we need to be careful:
                // - For overlapping positions (within 2 chars), treat them as the same
                // - For non-overlapping positions, keep them separate
                int beginDiff = Math.abs(a.getBeginPosition() - b.getBeginPosition());
                int endDiff = Math.abs(a.getEndPosition() - b.getEndPosition());
                
                // If positions overlap significantly, treat them as the same
                if (beginDiff <= 2 && endDiff <= 2) {
                    return 0; // Treat as equal/same position
                }
                
                // Otherwise, order by begin position then end position
                int beginCompare = Integer.compare(a.getBeginPosition(), b.getBeginPosition());
                if (beginCompare != 0) return beginCompare;
                
                return Integer.compare(a.getEndPosition(), b.getEndPosition());
            });

            // Add all positions to the set for deduplication
            positionSet.addAll(positions);
            positionSet.addAll(other.getPositions());

            // Clear and re-add all positions in sorted order
            positions.clear();
            positions.addAll(positionSet);

            if (logSampler.shouldLog()) {
                logger.debug("Merged {} + {} positions into {} unique positions",
                    initialSize, otherSize, positions.size());
            }
        } catch (Exception e) {
            logger.error("Failed to merge position lists: {}", e.getMessage(), e);
            throw e;
        }
    }

    public int size() {
        return positions.size();
    }

    public void sort() {
        Collections.sort(positions, (a, b) -> {
            int docCompare = Integer.compare(a.getDocumentId(), b.getDocumentId());
            if (docCompare != 0) return docCompare;
            
            int sentCompare = Integer.compare(a.getSentenceId(), b.getSentenceId());
            if (sentCompare != 0) return sentCompare;
            
            int beginCompare = Integer.compare(a.getBeginPosition(), b.getBeginPosition());
            if (beginCompare != 0) return beginCompare;
            
            return Integer.compare(a.getEndPosition(), b.getEndPosition());
        });
    }
}

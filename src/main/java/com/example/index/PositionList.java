package com.example.index;

import me.lemire.integercompression.*;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;

/**
 * Manages collections of Position objects with efficient compression and serialization capabilities.
 * Uses FastPFOR128 compression to minimize storage requirements while maintaining quick access times.
 * Supports operations like merging, sorting, and deduplication of positions. The class handles
 * serialization by separating position data into parallel arrays for optimal compression ratios,
 * making it suitable for storage in key-value databases. Thread-safe for read operations through
 * unmodifiable list views.
 */
public class PositionList {
    private final List<Position> positions;
    private static final IntegerCODEC codec = new FastPFOR128();
    
    public PositionList() {
        this.positions = new ArrayList<>();
    }

    public synchronized void add(Position position) {
        positions.add(position);
    }

    public List<Position> getPositions() {
        return Collections.unmodifiableList(positions);
    }

    public byte[] serialize() {
        if (positions.isEmpty()) {
            return new byte[0];
        }

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

        // Debug values before compression
        // System.out.println("Array size: " + positions.size());
        // System.out.println("Before compression - First docId: " + docIds[0]);
        // System.out.println("Sample values: " + docIds[0] + ", " + docIds[Math.min(1, docIds.length-1)]);

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
            
            // System.out.println(String.format(
            //     "Array: original=%d, padded=%d, compressed=%d, first=%d, last=%d",
            //     array.length, paddedSize, compressedSize, array[0], array[array.length-1]));
        }

        // Write timestamps separately (not compressed)
        for (long timestamp : timestamps) {
            buffer.putLong(timestamp);
        }

        // Create exact-sized result
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    public static PositionList deserialize(byte[] data) {
        if (data.length == 0) {
            return new PositionList();
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        PositionList result = new PositionList();

        // Read metadata
        int count = buffer.getInt();

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
            
            // System.out.println(String.format("Decompressed: length=%d, first=%d, last=%d",
            //     array.length, array[0], array[array.length-1]));
        }

        // Read timestamps
        for (int i = 0; i < count; i++) {
            timestamps[i] = buffer.getLong();
        }

        // After decompression
        // System.out.println("After decompression - First docId: " + docIds[0]);

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
    }

    public synchronized void merge(PositionList other) {
        Set<Position> positionSet = new HashSet<>(positions);
        positionSet.addAll(other.positions);
        positions.clear();
        positions.addAll(positionSet);
        sort();
    }

    public int size() {
        return positions.size();
    }

    public void sort() {
        positions.sort((a, b) -> {
            int docCompare = Integer.compare(a.getDocumentId(), b.getDocumentId());
            if (docCompare != 0) return docCompare;
            int sentCompare = Integer.compare(a.getSentenceId(), b.getSentenceId());
            if (sentCompare != 0) return sentCompare;
            return Integer.compare(a.getBeginPosition(), b.getBeginPosition());
        });
    }
}

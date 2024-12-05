package com.example.index;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class PositionSerializationTest {

    @Test
    public void testSinglePositionSerialization() {
        // Create a single position
        Position original = new Position(
                193, // documentId
                1, // sentenceId
                10, // beginPosition
                15, // endPosition
                LocalDate.of(1993, 10, 26) // timestamp
        );

        // Create PositionList with single position
        PositionList originalList = new PositionList();
        originalList.add(original);

        // Serialize and deserialize
        byte[] serialized = originalList.serialize();
        PositionList deserializedList = PositionList.deserialize(serialized);

        // Verify
        List<Position> positions = deserializedList.getPositions();
        assertEquals(1, positions.size(), "Should have exactly one position");

        Position deserialized = positions.get(0);
        assertEquals(original.getDocumentId(), deserialized.getDocumentId(), "Document ID mismatch");
        assertEquals(original.getSentenceId(), deserialized.getSentenceId(), "Sentence ID mismatch");
        assertEquals(original.getBeginPosition(), deserialized.getBeginPosition(), "Begin position mismatch");
        assertEquals(original.getEndPosition(), deserialized.getEndPosition(), "End position mismatch");
        assertEquals(original.getTimestamp(), deserialized.getTimestamp(), "Timestamp mismatch");
    }

    @Test
    public void testMultiplePositionsSerialization() {
        // Create test data
        PositionList originalList = new PositionList();
        originalList.add(new Position(193, 1, 1, 3, LocalDate.of(1993, 10, 26)));
        originalList.add(new Position(222, 3, 3, 5, LocalDate.of(1995, 4, 1)));
        originalList.add(new Position(3013, 1, 1, 3, LocalDate.of(2001, 9, 11)));
        originalList.add(new Position(9999, 15, 15, 18, LocalDate.of(2020, 4, 20)));

        // Serialize
        byte[] serialized = originalList.serialize();

        // Deserialize
        PositionList deserializedList = PositionList.deserialize(serialized);

        // Verify
        List<Position> original = originalList.getPositions();
        List<Position> deserialized = deserializedList.getPositions();

        assertEquals(original.size(), deserialized.size(), "Size mismatch");

        for (int i = 0; i < original.size(); i++) {
            Position op = original.get(i);
            Position dp = deserialized.get(i);

            assertEquals(op.getDocumentId(), dp.getDocumentId(),
                    "Document ID mismatch at index " + i);
            assertEquals(op.getSentenceId(), dp.getSentenceId(),
                    "Sentence ID mismatch at index " + i);
            assertEquals(op.getBeginPosition(), dp.getBeginPosition(),
                    "Begin position mismatch at index " + i);
            assertEquals(op.getEndPosition(), dp.getEndPosition(),
                    "End position mismatch at index " + i);
            assertEquals(op.getTimestamp(), dp.getTimestamp(),
                    "Timestamp mismatch at index " + i);
        }
    }

    @Test
    public void testEmptyListSerialization() {
        PositionList emptyList = new PositionList();
        byte[] serialized = emptyList.serialize();
        PositionList deserialized = PositionList.deserialize(serialized);

        assertEquals(0, deserialized.size(), "Deserialized empty list should have size 0");
    }

    @Test
    public void testLargeListSerialization() {
        PositionList largeList = new PositionList();
        int numPositions = 10000; // Test with 10,000 positions

        // Add many positions
        for (int i = 0; i < numPositions; i++) {
            largeList.add(new Position(
                    i, // documentId
                    i % 100, // sentenceId
                    i * 2, // beginPosition
                    i * 2 + 5, // endPosition
                    LocalDate.of(2000, 1, 1).plusDays(i % 365) // timestamp
            ));
        }

        // Serialize and deserialize
        byte[] serialized = largeList.serialize();
        PositionList deserialized = PositionList.deserialize(serialized);

        // Verify
        assertEquals(numPositions, deserialized.size(),
                "Should maintain size after serialization/deserialization");

        // Check a few random positions
        List<Position> originalPositions = largeList.getPositions();
        List<Position> deserializedPositions = deserialized.getPositions();

        for (int i : Arrays.asList(0, 999, 5000, 9999)) {
            Position op = originalPositions.get(i);
            Position dp = deserializedPositions.get(i);
            assertEquals(op.getDocumentId(), dp.getDocumentId(),
                    "Document ID mismatch at index " + i);
        }
    }

    @Test
    public void testPositionSorting() {
        PositionList list = new PositionList();

        // Add positions in random order
        list.add(new Position(2, 1, 5, 8, LocalDate.of(2000, 1, 1)));
        list.add(new Position(1, 2, 3, 6, LocalDate.of(2000, 1, 1)));
        list.add(new Position(1, 1, 7, 9, LocalDate.of(2000, 1, 1)));
        list.add(new Position(1, 1, 1, 4, LocalDate.of(2000, 1, 1)));

        // Sort
        list.sort();

        // Verify order
        List<Position> sorted = list.getPositions();
        assertEquals(1, sorted.get(0).getDocumentId(), "First position should be doc 1");
        assertEquals(1, sorted.get(0).getBeginPosition(), "Should be earliest position in doc 1");
        assertEquals(2, sorted.get(3).getDocumentId(), "Last position should be doc 2");
    }
}

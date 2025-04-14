package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.core.IndexAccessInterface;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import com.example.query.executor.QueryResult;
import com.example.query.model.Query;
import com.example.query.model.condition.Pos;

import org.iq80.leveldb.DBIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class PosConditionExecutorTest {

    @Mock private IndexAccess posIndex;
    @Mock private DBIterator posIterator;
    @Mock private ConditionExecutorFactory factory;
    @InjectMocks private PosExecutor executor;

    private Map<String, IndexAccessInterface> indexes;

    @BeforeEach
    void setUp() throws IndexAccessException {
        indexes = Map.of("pos", posIndex);
        lenient().when(posIndex.iterator()).thenReturn(posIterator);
        lenient().when(posIterator.hasNext()).thenReturn(false);
        lenient().when(posIndex.get(any(byte[].class))).thenReturn(Optional.empty());
    }

    @Test
    void testExecuteSpecificTermDocumentGranularity() throws Exception {
        Pos condition = new Pos("NN", "test"); 
        String expectedKey = "nn" + IndexAccessInterface.DELIMITER + "test";

        PositionList positions = new PositionList();
        positions.add(new Position(1, 0, 5, 10, LocalDate.now()));
        positions.add(new Position(2, 1, 15, 20, LocalDate.now()));
        positions.add(new Position(1, 1, 25, 30, LocalDate.now()));
        when(posIndex.get(eq(expectedKey.getBytes()))).thenReturn(Optional.of(positions));

        QueryResult result = executor.execute(condition, indexes, Query.Granularity.DOCUMENT, 0, "test_corpus");

        assertNotNull(result);
        assertEquals(Query.Granularity.DOCUMENT, result.getGranularity());
        assertEquals(3, result.getAllDetails().size());
        Set<Integer> docIds = result.getAllDetails().stream().map(MatchDetail::getDocumentId).collect(Collectors.toSet());
        assertTrue(docIds.containsAll(Set.of(1, 2)));
        assertTrue(result.getAllDetails().stream().allMatch(d -> "test/NN".equals(d.value()) && d.valueType() == ValueType.POS_TERM)); 
        assertNull(result.getAllDetails().get(0).variableName()); 
        verify(posIndex).get(eq(expectedKey.getBytes())); 
    }
    
    @Test
    void testExecuteSpecificTermSentenceGranularity() throws Exception {
        Pos condition = new Pos("VB", "run");
        String expectedKey = "vb" + IndexAccessInterface.DELIMITER + "run";

        PositionList positions = new PositionList();
        positions.add(new Position(1, 1, 1, 2, null));
        positions.add(new Position(1, 2, 3, 4, null));
        positions.add(new Position(2, 1, 5, 6, null));
        positions.add(new Position(1, 1, 10, 15, null));
        
        when(posIndex.get(eq(expectedKey.getBytes()))).thenReturn(Optional.of(positions));

        QueryResult result = executor.execute(condition, indexes, Query.Granularity.SENTENCE, 0, "test_corpus");
        
        assertNotNull(result);
        assertEquals(Query.Granularity.SENTENCE, result.getGranularity());
        assertEquals(4, result.getAllDetails().size());
        assertTrue(result.getAllDetails().stream().anyMatch(m -> m.getDocumentId() == 1 && m.getSentenceId() == 1 && "run/VB".equals(m.value())));
        assertTrue(result.getAllDetails().stream().anyMatch(m -> m.getDocumentId() == 1 && m.getSentenceId() == 2 && "run/VB".equals(m.value())));
        assertTrue(result.getAllDetails().stream().anyMatch(m -> m.getDocumentId() == 2 && m.getSentenceId() == 1 && "run/VB".equals(m.value())));
        assertTrue(result.getAllDetails().stream().allMatch(d -> d.valueType() == ValueType.POS_TERM));
        
        verify(posIndex).get(eq(expectedKey.getBytes()));
    }

    @Test
    void testSentenceGranularityWithWindow() throws Exception {
        Pos condition = new Pos("NN", "noun");
        String expectedKey = "nn" + IndexAccessInterface.DELIMITER + "noun";

        PositionList positions = new PositionList();
        positions.add(new Position(1, 0, 1, 2, null));
        positions.add(new Position(1, 2, 3, 4, null));
        positions.add(new Position(1, 3, 5, 6, null));
        positions.add(new Position(2, 1, 7, 8, null));
        
        when(posIndex.get(eq(expectedKey.getBytes()))).thenReturn(Optional.of(positions));

        QueryResult result = executor.execute(condition, indexes, Query.Granularity.SENTENCE, 1, "test_corpus"); 

        assertNotNull(result);
        assertEquals(Query.Granularity.SENTENCE, result.getGranularity());
        assertEquals(4, result.getAllDetails().size()); 
        
        assertTrue(result.getAllDetails().stream().anyMatch(m -> m.getDocumentId() == 1 && m.getSentenceId() == 0 && "noun/NN".equals(m.value())));
        assertTrue(result.getAllDetails().stream().anyMatch(m -> m.getDocumentId() == 1 && m.getSentenceId() == 2 && "noun/NN".equals(m.value())));
        assertTrue(result.getAllDetails().stream().anyMatch(m -> m.getDocumentId() == 2 && m.getSentenceId() == 1 && "noun/NN".equals(m.value())));

        assertTrue(result.getAllDetails().stream().allMatch(d -> d.valueType() == ValueType.POS_TERM));
        verify(posIndex).get(eq(expectedKey.getBytes()));
    }

    @Test
    void testVariableBindingDocumentGranularity() throws Exception {
        Pos condition = new Pos("JJ", null, "?adjVar"); 
        String expectedKeyPrefix = "jj" + IndexAccessInterface.DELIMITER;

        // Define the expected positions and corresponding keys for the prefix search
        PositionList posList1 = new PositionList(); posList1.add(new Position(1, 1, 5, 10, LocalDate.now()));
        PositionList posList2 = new PositionList(); posList2.add(new Position(2, 1, 15, 20, LocalDate.now()));
        PositionList posList3 = new PositionList(); posList3.add(new Position(1, 2, 25, 30, LocalDate.now()));
        
        // Mock iterator behavior for prefix search
        List<Map.Entry<byte[], PositionList>> mockEntries = List.of(
            Map.entry((expectedKeyPrefix + "happy").getBytes(), posList1),
            Map.entry((expectedKeyPrefix + "sad").getBytes(), posList2),
            Map.entry((expectedKeyPrefix + "glad").getBytes(), posList3) 
        );
        
        // Setup the mock iterator using a helper if available, or inline mocking
        // Assuming a similar helper `setupIteratorMock` exists or mocking directly:
        lenient().when(posIndex.iterator()).thenReturn(posIterator);
        byte[] prefixBytes = expectedKeyPrefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        doNothing().when(posIterator).seek(argThat(k -> Arrays.equals(k, prefixBytes)));

        // Mock hasNext and next calls based on mockEntries
        Boolean[] hasNextValues = new Boolean[mockEntries.size() + 1];
        Arrays.fill(hasNextValues, 0, mockEntries.size(), true);
        hasNextValues[mockEntries.size()] = false;
        org.mockito.stubbing.OngoingStubbing<Boolean> hasNextStubbing = when(posIterator.hasNext()).thenReturn(true);
        for (int i = 1; i < hasNextValues.length; i++) {
            hasNextStubbing = hasNextStubbing.thenReturn(hasNextValues[i]);
        }

        if (!mockEntries.isEmpty()) {
            @SuppressWarnings("unchecked")
            Map.Entry<byte[], byte[]>[] entryArray = mockEntries.stream()
                 .map(e -> Map.entry(e.getKey(), e.getValue().serialize()))
                 .toArray(Map.Entry[]::new);
            // Mock next() to return entries in sequence
            org.mockito.stubbing.OngoingStubbing<Map.Entry<byte[], byte[]>> nextStubbing = when(posIterator.next()).thenReturn(entryArray[0]);
            for (int i = 1; i < entryArray.length; i++) {
                nextStubbing = nextStubbing.thenReturn(entryArray[i]);
            }
            nextStubbing.thenThrow(new java.util.NoSuchElementException());
            // Since PosExecutor.executeVariableExtraction does NOT use peekNext, we don't mock it.
        } else {
            when(posIterator.next()).thenThrow(new java.util.NoSuchElementException());
        }

        // when(posIndex.get(eq(expectedKey.getBytes()))).thenReturn(Optional.of(positions)); // Remove this line
        
        QueryResult result = executor.execute(condition, indexes, Query.Granularity.DOCUMENT, 0, "test_corpus");

        assertNotNull(result);
        assertEquals(Query.Granularity.DOCUMENT, result.getGranularity());
        // We expect 3 details because there are 3 positions across 3 keys
        assertEquals(3, result.getAllDetails().size()); 
        
        Set<Integer> docIds = result.getAllDetails().stream().map(MatchDetail::getDocumentId).collect(Collectors.toSet());
        assertTrue(docIds.containsAll(Set.of(1, 2)), "Docs 1 and 2 should be present");
        
        assertTrue(result.getAllDetails().stream().allMatch(d -> "?adjVar".equals(d.variableName())), "Variable name mismatch");
        assertTrue(result.getAllDetails().stream().allMatch(d -> d.valueType() == ValueType.POS_TERM), "ValueType mismatch");

        // Check captured values (term/tag)
        Set<String> capturedValues = result.getAllDetails().stream().map(d -> (String) d.value()).collect(Collectors.toSet());
        assertTrue(capturedValues.containsAll(Set.of("happy/jj", "sad/jj", "glad/jj")), "Captured values mismatch");

        // Verify iterator interactions
        verify(posIndex).iterator(); 
        verify(posIterator).seek(argThat(k -> Arrays.equals(k, prefixBytes)));
        verify(posIterator, times(mockEntries.size() + 1)).hasNext(); // Called once per entry + final false
        verify(posIterator, times(mockEntries.size())).next(); // Called once per entry
        verify(posIndex, times(0)).get(any()); // Ensure get is NOT called
    }
} 
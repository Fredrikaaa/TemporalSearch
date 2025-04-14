package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.core.IndexAccessInterface;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.Query;
import com.example.query.model.condition.*;
import com.example.query.binding.MatchDetail;
import com.example.query.binding.ValueType;
import com.example.query.executor.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class LogicalConditionExecutorTest {

    @Mock private ConditionExecutorFactory mockFactory;
    @Mock private ContainsExecutor mockContainsExecutor;
    @Mock private PosExecutor mockPosExecutor;
    // Mock other executors if needed for more complex tests

    @Mock private Contains mockContainsCond1;
    @Mock private Pos mockPosCond2;

    private LogicalExecutor logicalExecutor;
    private Map<String, IndexAccessInterface> indexes;
    private final LocalDate testDate = LocalDate.now();
    private static final Query.Granularity TEST_GRANULARITY = Query.Granularity.SENTENCE;
    private static final int TEST_WINDOW_SIZE = 0;

    // Helper to create a simple MatchDetail
    private MatchDetail createDetail(int docId, int sentId, String varName, Object value, int begin, int end) {
        Position pos = new Position(docId, sentId, begin, end, testDate);
        // Determine ValueType based on simple inspection of value for testing
        ValueType type = (value instanceof String) ? ValueType.TERM : ValueType.ENTITY; 
        return new MatchDetail(value, type, pos, "cond_" + varName, varName);
    }

    @BeforeEach
    void setUp() {
        logicalExecutor = new LogicalExecutor(mockFactory);
        indexes = new HashMap<>(); // Add mock IndexAccess if needed by sub-executors

        // Basic factory stubbing - Return the mocks of concrete types
        // The factory method likely returns ConditionExecutor, but the mocked
        // instances (mockContainsExecutor, mockPosExecutor) are compatible.
    }

    @Test
    void testExecuteAnd() throws Exception {
        Logical condition = new Logical(Logical.LogicalOperator.AND, List.of(mockContainsCond1, mockPosCond2));
        String var1 = "?term";
        String var2 = "?posTag";
        String val1 = "apple";
        String val2 = "NN"; // POS tag value

        // Mock results using MatchDetail and QueryResult
        List<MatchDetail> containsDetails = List.of(
            createDetail(1, 1, var1, val1, 0, 5), // Match
            createDetail(1, 2, var1, val1, 10, 15),
            createDetail(2, 1, var1, val1, 0, 5)  // Match
        );
        QueryResult containsResult = new QueryResult(TEST_GRANULARITY, TEST_WINDOW_SIZE, containsDetails);

        List<MatchDetail> posDetails = List.of(
            createDetail(1, 1, var2, val2, 6, 10),  // Match
            createDetail(2, 1, var2, val2, 6, 10),  // Match
            createDetail(3, 1, var2, val2, 0, 5)
        );
        QueryResult posResult = new QueryResult(TEST_GRANULARITY, TEST_WINDOW_SIZE, posDetails);

        // Stub factory calls for this test
        when(mockFactory.getExecutor(eq(mockContainsCond1))).thenReturn(mockContainsExecutor);
        when(mockFactory.getExecutor(eq(mockPosCond2))).thenReturn(mockPosExecutor);

        // Stub sub-executor calls to return QueryResult directly
        when(mockContainsExecutor.execute(eq(mockContainsCond1), any(), any(Query.Granularity.class), anyInt(), anyString()))
            .thenReturn(containsResult);

        when(mockPosExecutor.execute(eq(mockPosCond2), any(), any(Query.Granularity.class), anyInt(), anyString()))
            .thenReturn(posResult);

        // Execute - Now returns QueryResult
        QueryResult finalResult = logicalExecutor.execute(condition, indexes, TEST_GRANULARITY, TEST_WINDOW_SIZE, "test_corpus");

        // Verify final result (intersection)
        assertNotNull(finalResult);
        assertEquals(TEST_GRANULARITY, finalResult.getGranularity());
        assertEquals(TEST_WINDOW_SIZE, finalResult.getGranularitySize());

        // Intersection should contain details from both sub-results for matching (docId, sentId) pairs
        // Doc 1, Sent 1: Should have ?term and ?posTag details
        // Doc 2, Sent 1: Should have ?term and ?posTag details
        // Other docs/sents should be excluded

        // Expected details in the intersection (combining details for matching doc/sent)
        List<MatchDetail> expectedDetails = List.of(
             createDetail(1, 1, var1, val1, 0, 5), createDetail(1, 1, var2, val2, 6, 10),
             createDetail(2, 1, var1, val1, 0, 5), createDetail(2, 1, var2, val2, 6, 10) 
        );
        
        // Use sets for comparison as order doesn't matter within QueryResult
        Set<MatchDetail> expectedDetailSet = new HashSet<>(expectedDetails);
        Set<MatchDetail> actualDetailSet = new HashSet<>(finalResult.getAllDetails());

        assertEquals(expectedDetailSet.size(), actualDetailSet.size());
        assertEquals(expectedDetailSet, actualDetailSet);

        // Verify bindings within QueryResult (optional, but good practice)
        Map<Integer, List<MatchDetail>> detailsByDoc = finalResult.getDetailsByDocId();
        assertTrue(detailsByDoc.containsKey(1));
        assertTrue(detailsByDoc.containsKey(2));
        assertFalse(detailsByDoc.containsKey(3)); // Doc 3 should be excluded

        // Check specific bindings for Doc 1
        List<MatchDetail> doc1Details = detailsByDoc.get(1);
        assertTrue(doc1Details.stream().anyMatch(d -> var1.equals(d.variableName()) && val1.equals(d.value())));
        assertTrue(doc1Details.stream().anyMatch(d -> var2.equals(d.variableName()) && val2.equals(d.value())));
    }

    @Test
    void testExecuteOr() throws Exception {
        Logical condition = new Logical(Logical.LogicalOperator.OR, List.of(mockContainsCond1, mockPosCond2));
        String var1 = "?term";
        String var2 = "?posTag";
        String val1 = "apple";
        String val2 = "NN";

        // Mock results using MatchDetail and QueryResult
        List<MatchDetail> containsDetails = List.of(
            createDetail(1, 1, var1, val1, 0, 5),   // Overlap
            createDetail(1, 2, var1, val1, 10, 15) // Only Contains
        );
        QueryResult containsResult = new QueryResult(TEST_GRANULARITY, TEST_WINDOW_SIZE, containsDetails);

        List<MatchDetail> posDetails = List.of(
            createDetail(1, 1, var2, val2, 6, 10),  // Overlap
            createDetail(3, 1, var2, val2, 0, 5)   // Only POS
        );
        QueryResult posResult = new QueryResult(TEST_GRANULARITY, TEST_WINDOW_SIZE, posDetails);

        // Stub factory calls for this test
        when(mockFactory.getExecutor(eq(mockContainsCond1))).thenReturn(mockContainsExecutor);
        when(mockFactory.getExecutor(eq(mockPosCond2))).thenReturn(mockPosExecutor);

        // Stub sub-executor calls
        when(mockContainsExecutor.execute(eq(mockContainsCond1), any(), any(Query.Granularity.class), anyInt(), anyString()))
            .thenReturn(containsResult);
        when(mockPosExecutor.execute(eq(mockPosCond2), any(), any(Query.Granularity.class), anyInt(), anyString()))
            .thenReturn(posResult);

        // Execute
        QueryResult finalResult = logicalExecutor.execute(condition, indexes, TEST_GRANULARITY, TEST_WINDOW_SIZE, "test_corpus");

        // Verify final result (union)
        assertNotNull(finalResult);
        assertEquals(TEST_GRANULARITY, finalResult.getGranularity());
        assertEquals(TEST_WINDOW_SIZE, finalResult.getGranularitySize());

        // Expected details (union of inputs, duplicates handled by Set conversion)
        List<MatchDetail> expectedDetails = List.of(
            createDetail(1, 1, var1, val1, 0, 5), createDetail(1, 1, var2, val2, 6, 10), // Combined from overlap
            createDetail(1, 2, var1, val1, 10, 15),
            createDetail(3, 1, var2, val2, 0, 5)
        );
        Set<MatchDetail> expectedDetailSet = new HashSet<>(expectedDetails);
        Set<MatchDetail> actualDetailSet = new HashSet<>(finalResult.getAllDetails());

        assertEquals(expectedDetailSet.size(), actualDetailSet.size());
        assertEquals(expectedDetailSet, actualDetailSet);

        // Verify specific details are present
        assertTrue(actualDetailSet.stream().anyMatch(d -> d.getDocumentId() == 1 && d.getSentenceId() == 1 && var1.equals(d.variableName())));
        assertTrue(actualDetailSet.stream().anyMatch(d -> d.getDocumentId() == 1 && d.getSentenceId() == 1 && var2.equals(d.variableName())));
        assertTrue(actualDetailSet.stream().anyMatch(d -> d.getDocumentId() == 1 && d.getSentenceId() == 2 && var1.equals(d.variableName())));
        assertTrue(actualDetailSet.stream().anyMatch(d -> d.getDocumentId() == 3 && d.getSentenceId() == 1 && var2.equals(d.variableName())));
    }

    @Test
    void testExecuteAndShortCircuit() throws QueryExecutionException {
        Logical condition = new Logical(Logical.LogicalOperator.AND, List.of(mockContainsCond1, mockPosCond2));

        // Stub factory calls for this test
        when(mockFactory.getExecutor(eq(mockContainsCond1))).thenReturn(mockContainsExecutor);

        // Mock first executor returns empty QueryResult
        when(mockContainsExecutor.execute(eq(mockContainsCond1), eq(indexes), eq(TEST_GRANULARITY), eq(TEST_WINDOW_SIZE), anyString()))
            .thenReturn(new QueryResult(TEST_GRANULARITY, TEST_WINDOW_SIZE, Collections.emptyList()));

        // Setup second executor leniently (should not be called due to short-circuit)
        verify(mockPosExecutor, never()).execute(eq(mockPosCond2), eq(indexes), eq(TEST_GRANULARITY), eq(TEST_WINDOW_SIZE), anyString());

        // Execute
        QueryResult finalResult = logicalExecutor.execute(condition, indexes, TEST_GRANULARITY, TEST_WINDOW_SIZE, "test_corpus");

        // Verify
        assertNotNull(finalResult);
        assertTrue(finalResult.getAllDetails().isEmpty(), "Result should be empty due to short-circuit");
        
        // Verify first executor was called
        verify(mockContainsExecutor).execute(eq(mockContainsCond1), eq(indexes), eq(TEST_GRANULARITY), eq(TEST_WINDOW_SIZE), anyString());
    }

    @Test
    void testExecuteEmptySubconditions() throws QueryExecutionException {
        Logical condition = new Logical(Logical.LogicalOperator.OR, Collections.emptyList());

        // Execute should handle empty conditions gracefully (return empty result)
        QueryResult result = logicalExecutor.execute(condition, indexes, TEST_GRANULARITY, TEST_WINDOW_SIZE, "test_corpus");

        assertNotNull(result);
        assertTrue(result.getAllDetails().isEmpty(), "Expected empty result for empty conditions");
        // Verify factory was not called
        verify(mockFactory, never()).getExecutor(any());
    }

} 
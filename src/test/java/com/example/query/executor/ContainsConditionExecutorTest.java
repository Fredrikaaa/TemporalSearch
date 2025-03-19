package com.example.query.executor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.binding.BindingContext;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Contains;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@ExtendWith(MockitoExtension.class)
public class ContainsConditionExecutorTest {

    @Mock private IndexAccess mockUnigramIndex;
    @Mock private IndexAccess mockBigramIndex;
    @Mock private IndexAccess mockTrigramIndex;
    
    private ContainsExecutor executor;
    private Map<String, IndexAccess> indexes;
    private BindingContext bindingContext;
    
    @BeforeEach
    void setUp() {
        executor = new ContainsExecutor();
        
        indexes = new HashMap<>();
        indexes.put("unigram", mockUnigramIndex);
        indexes.put("bigram", mockBigramIndex);
        indexes.put("trigram", mockTrigramIndex);
        
        bindingContext = BindingContext.empty();
    }
    
    @Test
    void testExecuteSingleTerm() throws Exception {
        // Setup
        Contains condition = new Contains("test");
        PositionList positionList = new PositionList();
        positionList.add(new Position(1, 1, 0, 4, LocalDate.now()));
        positionList.add(new Position(2, 1, 5, 9, LocalDate.now()));
        
        // Expect unigram index to be used for single term
        when(mockUnigramIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify
        verify(mockUnigramIndex).get(any());
        assertEquals(2, result.size());
        
        // Extract document IDs for verification
        Set<Integer> docIds = result.stream()
                .map(DocSentenceMatch::documentId)
                .collect(Collectors.toSet());
        
        assertTrue(docIds.contains(1));
        assertTrue(docIds.contains(2));
    }
    
    @Test
    void testExecuteMultipleTerms() throws Exception {
        // Setup - now we expect a bigram search with two terms
        Contains condition = new Contains(Arrays.asList("test", "example"));
        
        PositionList positionList = new PositionList();
        positionList.add(new Position(1, 1, 0, 12, LocalDate.now()));
        positionList.add(new Position(2, 1, 5, 17, LocalDate.now()));
        
        // Expect bigram index to be used with the combined terms
        when(mockBigramIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify
        verify(mockBigramIndex).get(any());
        
        assertEquals(2, result.size());
        
        // Extract document IDs for verification
        Set<Integer> docIds = result.stream()
                .map(DocSentenceMatch::documentId)
                .collect(Collectors.toSet());
        
        assertTrue(docIds.contains(1));
        assertTrue(docIds.contains(2));
    }
    
    @Test
    void testExecuteWithBigramIndex() throws Exception {
        // This test is now redundant with testExecuteMultipleTerms
        // But we'll keep it with a different condition to test the same functionality
        
        // Setup - using a list of terms instead of a space-separated string
        Contains condition = new Contains(Arrays.asList("another", "test"));
        PositionList positionList = new PositionList();
        positionList.add(new Position(1, 1, 0, 12, LocalDate.now()));
        positionList.add(new Position(2, 1, 5, 17, LocalDate.now()));
        
        // Expect bigram index to be used
        when(mockBigramIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify
        verify(mockBigramIndex).get(any());
        
        assertEquals(2, result.size());
        
        // Extract document IDs for verification
        Set<Integer> docIds = result.stream()
                .map(DocSentenceMatch::documentId)
                .collect(Collectors.toSet());
        
        assertTrue(docIds.contains(1));
        assertTrue(docIds.contains(2));
    }
    
    @Test
    void testExecuteWithTrigramIndex() throws Exception {
        // Setup - using a list of three terms
        Contains condition = new Contains(Arrays.asList("test", "example", "phrase"));
        PositionList positionList = new PositionList();
        positionList.add(new Position(1, 1, 0, 19, LocalDate.now()));
        positionList.add(new Position(2, 1, 5, 24, LocalDate.now()));
        
        // Expect trigram index to be used
        when(mockTrigramIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify
        verify(mockTrigramIndex).get(any());
        
        assertEquals(2, result.size());
        
        // Extract document IDs for verification
        Set<Integer> docIds = result.stream()
                .map(DocSentenceMatch::documentId)
                .collect(Collectors.toSet());
        
        assertTrue(docIds.contains(1));
        assertTrue(docIds.contains(2));
    }
    
    @Test
    void testExecuteWithWildcard() throws Exception {
        // Setup - using a wildcard in a bigram
        Contains condition = new Contains(Arrays.asList("test", "*"));
        
        // Since wildcards aren't fully implemented, we expect an empty result
        // This test will need to be updated when wildcard support is implemented
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify
        assertTrue(result.isEmpty());
    }
    
    @Test
    void testExecuteTermNotFound() throws Exception {
        // Setup
        Contains condition = new Contains("nonexistent");
        
        // Term not found in index
        when(mockUnigramIndex.get(any())).thenReturn(Optional.empty());
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify
        verify(mockUnigramIndex).get(any());
        assertTrue(result.isEmpty());
    }
    
    @Test
    void testExecuteTooManyTerms() {
        // Setup - more than 3 terms should throw an exception
        Contains condition = new Contains(Arrays.asList("one", "two", "three", "four"));
        
        // Execute and verify exception
        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0)
        );
        
        assertEquals(QueryExecutionException.ErrorType.INVALID_CONDITION, exception.getErrorType());
    }
    
    @Test
    void testExecuteMissingIndex() throws QueryExecutionException {
        // Setup
        Contains condition = new Contains("test");
        Map<String, IndexAccess> emptyIndexes = new HashMap<>();
        
        // Execute and verify exception
        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> executor.execute(condition, emptyIndexes, bindingContext, Query.Granularity.DOCUMENT, 1)
        );
        
        // Verify the exception details
        assertEquals(QueryExecutionException.ErrorType.MISSING_INDEX, exception.getErrorType());
        assertTrue(exception.getMessage().contains("Missing required unigram index"));
    }
    
    @Test
    void testDebugExecuteMultipleTerms() throws Exception {
        // Setup - now we expect a bigram search with two terms
        Contains condition = new Contains(Arrays.asList("test", "example"));
        
        PositionList positionList = new PositionList();
        positionList.add(new Position(1, 1, 0, 12, LocalDate.now()));
        positionList.add(new Position(2, 1, 5, 17, LocalDate.now()));
        
        // Use lenient stubbing to avoid UnnecessaryStubbingException
        lenient().when(mockUnigramIndex.get(any())).thenReturn(Optional.of(positionList));
        lenient().when(mockBigramIndex.get(any())).thenReturn(Optional.of(positionList));
        lenient().when(mockTrigramIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute with document granularity
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify interactions with all indexes
        System.out.println("Unigram interactions: " + mockingDetails(mockUnigramIndex).getInvocations().size());
        System.out.println("Bigram interactions: " + mockingDetails(mockBigramIndex).getInvocations().size());
        System.out.println("Trigram interactions: " + mockingDetails(mockTrigramIndex).getInvocations().size());
        
        // No assertions - this is just for debugging
    }
} 
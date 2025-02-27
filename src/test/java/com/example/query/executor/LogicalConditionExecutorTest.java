package com.example.query.executor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.query.model.Condition;
import com.example.query.model.ContainsCondition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.LogicalCondition;
import com.example.query.model.LogicalCondition.LogicalOperator;
import com.example.query.model.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ExtendWith(MockitoExtension.class)
public class LogicalConditionExecutorTest {

    @Mock private ConditionExecutorFactory executorFactory;
    @Mock private ConditionExecutor<Condition> conditionExecutor;
    
    private LogicalConditionExecutor logicalExecutor;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        logicalExecutor = new LogicalConditionExecutor(executorFactory);
    }
    
    @Test
    void testDocumentLevelIntersection() {
        // Create two sets of document-level matches
        Set<DocSentenceMatch> set1 = createDocumentMatches(1, 2, 3);
        Set<DocSentenceMatch> set2 = createDocumentMatches(2, 3, 4);
        
        // Add some position information
        addPositions(set1);
        addPositions(set2);

        // Intersect the sets
        Set<DocSentenceMatch> result = logicalExecutor.intersectMatches(set1, set2);

        // Verify result contains only documents in both sets
        assertEquals(2, result.size());
        assertTrue(containsDocumentId(result, 2));
        assertTrue(containsDocumentId(result, 3));
        assertFalse(containsDocumentId(result, 1));
        assertFalse(containsDocumentId(result, 4));
        
        // Verify position information was combined
        for (DocSentenceMatch match : result) {
            assertTrue(match.getPositions("key1").size() > 0);
            assertTrue(match.getPositions("key2").size() > 0);
        }
    }
    
    @Test
    void testSentenceLevelIntersection() {
        // Create two sets of sentence-level matches
        Set<DocSentenceMatch> set1 = createSentenceMatches(
            new int[][]{{1, 1}, {1, 2}, {2, 1}, {2, 2}});
        Set<DocSentenceMatch> set2 = createSentenceMatches(
            new int[][]{{1, 2}, {1, 3}, {2, 2}, {3, 1}});
        
        // Add some position information
        addPositions(set1);
        addPositions(set2);

        // Intersect the sets
        Set<DocSentenceMatch> result = logicalExecutor.intersectMatches(set1, set2);

        // Verify result contains only sentences in both sets
        assertEquals(2, result.size());
        assertTrue(containsSentence(result, 1, 2));
        assertTrue(containsSentence(result, 2, 2));
        assertFalse(containsSentence(result, 1, 1));
        assertFalse(containsSentence(result, 1, 3));
        
        // Verify position information was combined
        for (DocSentenceMatch match : result) {
            assertTrue(match.getPositions("key1").size() > 0);
            assertTrue(match.getPositions("key2").size() > 0);
        }
    }
    
    @Test
    void testDocumentLevelUnion() {
        // Create two sets of document-level matches
        Set<DocSentenceMatch> set1 = createDocumentMatches(1, 2);
        Set<DocSentenceMatch> set2 = createDocumentMatches(2, 3);
        
        // Add some position information
        addPositions(set1);
        addPositions(set2);

        // Union the sets
        Set<DocSentenceMatch> result = logicalExecutor.unionMatches(set1, set2);

        // Verify result contains all documents from both sets
        assertEquals(3, result.size());
        assertTrue(containsDocumentId(result, 1));
        assertTrue(containsDocumentId(result, 2));
        assertTrue(containsDocumentId(result, 3));
        
        // Verify position information was preserved
        for (DocSentenceMatch match : result) {
            if (match.getDocumentId() == 2) {
                // Document 2 should have positions from both sets
                assertTrue(match.getPositions("key1").size() > 0);
                assertTrue(match.getPositions("key2").size() > 0);
            } else {
                // Documents 1 and 3 should have positions from one set
                assertTrue(match.getPositions("key1").size() > 0 || match.getPositions("key2").size() > 0);
            }
        }
    }
    
    @Test
    void testSentenceLevelUnion() {
        // Create two sets of sentence-level matches
        Set<DocSentenceMatch> set1 = createSentenceMatches(
            new int[][]{{1, 1}, {1, 2}});
        Set<DocSentenceMatch> set2 = createSentenceMatches(
            new int[][]{{1, 2}, {1, 3}});
        
        // Add some position information
        addPositions(set1);
        addPositions(set2);

        // Union the sets
        Set<DocSentenceMatch> result = logicalExecutor.unionMatches(set1, set2);

        // Verify result contains all sentences from both sets
        assertEquals(3, result.size());
        assertTrue(containsSentence(result, 1, 1));
        assertTrue(containsSentence(result, 1, 2));
        assertTrue(containsSentence(result, 1, 3));
        
        // Verify position information was preserved
        for (DocSentenceMatch match : result) {
            if (match.getDocumentId() == 1 && match.getSentenceId() == 2) {
                // Sentence 1,2 should have positions from both sets
                assertTrue(match.getPositions("key1").size() > 0);
                assertTrue(match.getPositions("key2").size() > 0);
            } else {
                // Other sentences should have positions from one set
                assertTrue(match.getPositions("key1").size() > 0 || match.getPositions("key2").size() > 0);
            }
        }
    }
    
    @Test
    void testExecuteAnd() throws Exception {
        // Create conditions
        ContainsCondition condition1 = new ContainsCondition("test1");
        ContainsCondition condition2 = new ContainsCondition("test2");
        LogicalCondition andCondition = new LogicalCondition(
            LogicalOperator.AND, Arrays.asList(condition1, condition2));
        
        // Create result sets
        Set<DocSentenceMatch> result1 = createDocumentMatches(1, 2, 3);
        Set<DocSentenceMatch> result2 = createDocumentMatches(2, 3, 4);
        
        // Add position information
        addPositions(result1);
        addPositions(result2);
        
        // Set up mock to return our result sets
        when(executorFactory.getExecutor(any(Condition.class))).thenReturn(conditionExecutor);
        when(conditionExecutor.execute(any(Condition.class), any(Map.class), any(VariableBindings.class), any(Query.Granularity.class)))
            .thenReturn(result1)
            .thenReturn(result2);
        
        // Execute the AND condition
        Set<DocSentenceMatch> result = logicalExecutor.execute(
            andCondition, new HashMap<>(), new VariableBindings(), Query.Granularity.DOCUMENT);
        
        // Verify result
        assertEquals(2, result.size());
        assertTrue(containsDocumentId(result, 2));
        assertTrue(containsDocumentId(result, 3));
        assertFalse(containsDocumentId(result, 1));
        assertFalse(containsDocumentId(result, 4));
    }
    
    @Test
    void testExecuteOr() throws Exception {
        // Create conditions
        ContainsCondition condition1 = new ContainsCondition("test1");
        ContainsCondition condition2 = new ContainsCondition("test2");
        LogicalCondition orCondition = new LogicalCondition(
            LogicalOperator.OR, Arrays.asList(condition1, condition2));
        
        // Create result sets
        Set<DocSentenceMatch> result1 = createDocumentMatches(1, 2);
        Set<DocSentenceMatch> result2 = createDocumentMatches(2, 3);
        
        // Add position information
        addPositions(result1);
        addPositions(result2);
        
        // Set up mock to return our result sets
        when(executorFactory.getExecutor(any(Condition.class))).thenReturn(conditionExecutor);
        when(conditionExecutor.execute(any(Condition.class), any(Map.class), any(VariableBindings.class), any(Query.Granularity.class)))
            .thenReturn(result1)
            .thenReturn(result2);
        
        // Execute the OR condition
        Set<DocSentenceMatch> result = logicalExecutor.execute(
            orCondition, new HashMap<>(), new VariableBindings(), Query.Granularity.DOCUMENT);
        
        // Verify result
        assertEquals(3, result.size());
        assertTrue(containsDocumentId(result, 1));
        assertTrue(containsDocumentId(result, 2));
        assertTrue(containsDocumentId(result, 3));
    }
    
    // Helper methods
    
    private Set<DocSentenceMatch> createDocumentMatches(int... documentIds) {
        Set<DocSentenceMatch> matches = new HashSet<>();
        for (int docId : documentIds) {
            matches.add(new DocSentenceMatch(docId));
        }
        return matches;
    }
    
    private Set<DocSentenceMatch> createSentenceMatches(int[][] docSentencePairs) {
        Set<DocSentenceMatch> matches = new HashSet<>();
        for (int[] pair : docSentencePairs) {
            matches.add(new DocSentenceMatch(pair[0], pair[1]));
        }
        return matches;
    }
    
    private void addPositions(Set<DocSentenceMatch> matches) {
        LocalDate now = LocalDate.now();
        int i = 0;
        for (DocSentenceMatch match : matches) {
            if (i % 2 == 0) {
                if (match.isSentenceLevel()) {
                    Position pos = new Position(match.getDocumentId(), match.getSentenceId(), 10, 15, now);
                    match.addPosition("key1", pos);
                } else {
                    Position pos = new Position(match.getDocumentId(), 0, 10, 15, now);
                    match.addPosition("key1", pos);
                }
            } else {
                if (match.isSentenceLevel()) {
                    Position pos = new Position(match.getDocumentId(), match.getSentenceId(), 20, 25, now);
                    match.addPosition("key2", pos);
                } else {
                    Position pos = new Position(match.getDocumentId(), 0, 20, 25, now);
                    match.addPosition("key2", pos);
                }
            }
            i++;
        }
    }
    
    private boolean containsDocumentId(Set<DocSentenceMatch> matches, int documentId) {
        return matches.stream().anyMatch(match -> match.getDocumentId() == documentId);
    }
    
    private boolean containsSentence(Set<DocSentenceMatch> matches, int documentId, int sentenceId) {
        return matches.stream().anyMatch(match -> 
            match.getDocumentId() == documentId && match.getSentenceId() == sentenceId);
    }
} 
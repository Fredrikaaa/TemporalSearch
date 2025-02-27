package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.query.model.Condition;
import com.example.query.model.ContainsCondition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.NotCondition;
import com.example.query.model.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class NotConditionExecutorTest {

    @Mock
    private ConditionExecutorFactory executorFactory;
    
    @Mock
    private ConditionExecutor<Condition> conditionExecutor;
    
    @Mock
    private IndexAccess indexAccess;
    
    private NotConditionExecutor notExecutor;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        notExecutor = new NotConditionExecutor(executorFactory);
    }
    
    @Test
    void testNegateMatchesDocumentLevel() {
        // Create universe and toNegate sets
        Set<DocSentenceMatch> universe = createDocumentMatches(1, 2, 3, 4, 5);
        Set<DocSentenceMatch> toNegate = createDocumentMatches(2, 4);
        
        // Add position information
        addPositions(universe);
        addPositions(toNegate);
        
        // Negate the matches
        Set<DocSentenceMatch> result = notExecutor.negateMatches(universe, toNegate);
        
        // Verify result contains only documents not in toNegate
        assertEquals(3, result.size());
        assertTrue(containsDocumentId(result, 1));
        assertTrue(containsDocumentId(result, 3));
        assertTrue(containsDocumentId(result, 5));
        assertFalse(containsDocumentId(result, 2));
        assertFalse(containsDocumentId(result, 4));
    }
    
    @Test
    void testNegateMatchesSentenceLevel() {
        // Create universe and toNegate sets
        Set<DocSentenceMatch> universe = createSentenceMatches(
            new int[][]{{1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}});
        Set<DocSentenceMatch> toNegate = createSentenceMatches(
            new int[][]{{1, 2}, {1, 4}});
        
        // Add position information
        addPositions(universe);
        addPositions(toNegate);
        
        // Negate the matches
        Set<DocSentenceMatch> result = notExecutor.negateMatches(universe, toNegate);
        
        // Verify result contains only sentences not in toNegate
        assertEquals(3, result.size());
        assertTrue(containsSentence(result, 1, 1));
        assertTrue(containsSentence(result, 1, 3));
        assertTrue(containsSentence(result, 1, 5));
        assertFalse(containsSentence(result, 1, 2));
        assertFalse(containsSentence(result, 1, 4));
    }
    
    // We'll skip testing the execute method directly since it relies on private methods
    // that are difficult to mock. Instead, we'll focus on testing the negateMatches method
    // which is the core functionality.
    
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
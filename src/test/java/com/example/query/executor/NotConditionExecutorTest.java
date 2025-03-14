package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Not;
import org.iq80.leveldb.DBIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class NotConditionExecutorTest {
    
    @Mock
    private IndexAccess unigramIndex;
    
    @Mock
    private DBIterator iterator;
    
    private Map<String, IndexAccess> indexes;
    private NotExecutor notExecutor;
    private ConditionExecutorFactory executorFactory;
    
    @BeforeEach
    void setUp() throws IndexAccessException {
        MockitoAnnotations.openMocks(this);
        indexes = new HashMap<>();
        indexes.put("unigram", unigramIndex);
        executorFactory = new ConditionExecutorFactory();
        notExecutor = new NotExecutor(executorFactory);
        
        // Mock iterator behavior
        when(unigramIndex.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);
    }
    
    @Test
    void testNegateMatchesDocumentLevel() throws QueryExecutionException, IndexAccessException {
        // Setup test data for "test" word
        PositionList positions = new PositionList();
        positions.add(new Position(1, 1, 0, 5, LocalDate.now()));
        positions.add(new Position(2, 1, 0, 5, LocalDate.now()));
        
        // Mock index responses
        when(unigramIndex.get("test".getBytes())).thenReturn(Optional.of(positions));
        
        // Create a NOT condition
        Contains containsCondition = new Contains("test");
        Not notCondition = new Not(containsCondition);
        
        // Execute the NOT condition
        Set<DocSentenceMatch> results = notExecutor.execute(
            notCondition, 
            indexes, 
            new VariableBindings(),
            Query.Granularity.DOCUMENT,
            1
        );
        
        // Verify results - should not include docs 1 and 2
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 1));
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 2));
    }
    
    @Test
    void testNegateMatchesSentenceLevel() throws QueryExecutionException, IndexAccessException {
        // Setup test data for "test" word
        PositionList positions = new PositionList();
        positions.add(new Position(1, 1, 0, 5, LocalDate.now()));
        positions.add(new Position(1, 2, 0, 5, LocalDate.now()));
        positions.add(new Position(2, 1, 0, 5, LocalDate.now()));
        
        // Mock index responses
        when(unigramIndex.get("test".getBytes())).thenReturn(Optional.of(positions));
        
        // Create a NOT condition
        Contains containsCondition = new Contains("test");
        Not notCondition = new Not(containsCondition);
        
        // Execute the NOT condition
        Set<DocSentenceMatch> results = notExecutor.execute(
            notCondition, 
            indexes, 
            new VariableBindings(),
            Query.Granularity.SENTENCE,
            1
        );
        
        // Verify results - should not include doc 1 sentences 1,2 and doc 2 sentence 1
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 1 && m.sentenceId() == 1));
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 1 && m.sentenceId() == 2));
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 2 && m.sentenceId() == 1));
    }
} 
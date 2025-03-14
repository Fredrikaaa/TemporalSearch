package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.IndexAccessException;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Ner;
import com.example.query.model.condition.Not;
import org.iq80.leveldb.DBIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryExecutorTest {
    
    @Mock private IndexAccess unigramIndex;
    @Mock private IndexAccess nerIndex;
    @Mock private DBIterator unigramIterator;
    @Mock private DBIterator nerIterator;
    private Map<String, IndexAccess> indexes;
    private QueryExecutor queryExecutor;
    
    @BeforeEach
    void setUp() throws IndexAccessException {
        indexes = new HashMap<>();
        indexes.put("unigram", unigramIndex);
        indexes.put("ner", nerIndex);
        queryExecutor = new QueryExecutor(new ConditionExecutorFactory());
        
        // Mock iterator behavior with lenient mode
        lenient().when(unigramIndex.iterator()).thenReturn(unigramIterator);
        lenient().when(nerIndex.iterator()).thenReturn(nerIterator);
        lenient().when(unigramIterator.hasNext()).thenReturn(false);
        lenient().when(nerIterator.hasNext()).thenReturn(false);
    }
    
    @Test
    void testLogicalAndOperation() throws QueryExecutionException, IndexAccessException {
        // Setup test data for "test" word
        PositionList containsPositions = new PositionList();
        containsPositions.add(new Position(1, 1, 0, 5, LocalDate.now()));
        containsPositions.add(new Position(2, 1, 0, 5, LocalDate.now()));
        containsPositions.add(new Position(3, 1, 0, 5, LocalDate.now()));
        
        // Setup test data for PERSON NER
        PositionList nerPositions = new PositionList();
        nerPositions.add(new Position(2, 1, 10, 15, LocalDate.now()));
        nerPositions.add(new Position(3, 1, 10, 15, LocalDate.now()));
        
        // Mock index responses
        when(unigramIndex.get("test".getBytes())).thenReturn(Optional.of(containsPositions));
        when(nerIndex.get("PERSON|".getBytes())).thenReturn(Optional.of(nerPositions));
        
        // Create a query with AND condition
        Contains containsCondition = new Contains("test");
        Ner nerCondition = Ner.of("PERSON");
        Logical andCondition = new Logical(
            Logical.LogicalOperator.AND,
            Arrays.asList(containsCondition, nerCondition)
        );
        
        Query query = new Query(
            "test_source",
            Collections.singletonList(andCondition),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            Collections.emptyList()
        );
        
        // Execute query
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results - should only include docs that match both conditions (2 and 3)
        assertEquals(2, results.size());
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 2));
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 3));
    }
    
    @Test
    void testLogicalOrOperation() throws QueryExecutionException, IndexAccessException {
        // Setup test data for "test" word
        PositionList containsPositions = new PositionList();
        containsPositions.add(new Position(1, 1, 0, 5, LocalDate.now()));
        containsPositions.add(new Position(2, 1, 0, 5, LocalDate.now()));
        
        // Setup test data for PERSON NER
        PositionList nerPositions = new PositionList();
        nerPositions.add(new Position(2, 1, 10, 15, LocalDate.now()));
        nerPositions.add(new Position(3, 1, 10, 15, LocalDate.now()));
        
        // Mock index responses
        when(unigramIndex.get("test".getBytes())).thenReturn(Optional.of(containsPositions));
        when(nerIndex.get("PERSON|".getBytes())).thenReturn(Optional.of(nerPositions));
        
        // Create a query with OR condition
        Contains containsCondition = new Contains("test");
        Ner nerCondition = Ner.of("PERSON");
        Logical orCondition = new Logical(
            Logical.LogicalOperator.OR,
            Arrays.asList(containsCondition, nerCondition)
        );
        
        Query query = new Query(
            "test_source",
            Collections.singletonList(orCondition),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            Collections.emptyList()
        );
        
        // Execute query
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results - should include docs that match either condition (1, 2, and 3)
        assertEquals(3, results.size());
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 1));
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 2));
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 3));
    }
    
    @Test
    void testNotOperation() throws QueryExecutionException, IndexAccessException {
        // Setup test data for "test" word
        PositionList containsPositions = new PositionList();
        containsPositions.add(new Position(1, 1, 0, 5, LocalDate.now()));
        containsPositions.add(new Position(2, 1, 0, 5, LocalDate.now()));
        
        // Mock index responses
        when(unigramIndex.get("test".getBytes())).thenReturn(Optional.of(containsPositions));
        
        // Create a query with NOT condition
        Contains containsCondition = new Contains("test");
        Not notCondition = new Not(containsCondition);
        
        Query query = new Query(
            "test_source",
            Collections.singletonList(notCondition),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            Collections.emptyList()
        );
        
        // Execute query
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results - should not include docs 1 and 2
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 1));
        assertFalse(results.stream().anyMatch(m -> m.documentId() == 2));
    }
    
    @Test
    void testComplexLogicalOperation() throws QueryExecutionException, IndexAccessException {
        // Setup test data for "test" word
        PositionList testPositions = new PositionList();
        testPositions.add(new Position(1, 1, 0, 5, LocalDate.now()));
        testPositions.add(new Position(2, 1, 0, 5, LocalDate.now()));
        
        // Setup test data for "example" word
        PositionList examplePositions = new PositionList();
        examplePositions.add(new Position(2, 1, 10, 15, LocalDate.now()));
        examplePositions.add(new Position(3, 1, 10, 15, LocalDate.now()));
        
        // Mock index responses
        when(unigramIndex.get("test".getBytes())).thenReturn(Optional.of(testPositions));
        when(unigramIndex.get("example".getBytes())).thenReturn(Optional.of(examplePositions));
        
        // Create a complex query: (test AND example) OR NOT(test)
        Contains testCondition = new Contains("test");
        Contains exampleCondition = new Contains("example");
        
        Logical andCondition = new Logical(
            Logical.LogicalOperator.AND,
            Arrays.asList(testCondition, exampleCondition)
        );
        
        Not notTestCondition = new Not(testCondition);
        
        Logical orCondition = new Logical(
            Logical.LogicalOperator.OR,
            Arrays.asList(andCondition, notTestCondition)
        );
        
        Query query = new Query(
            "test_source",
            Collections.singletonList(orCondition),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            Collections.emptyList()
        );
        
        // Execute query
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Document 2 matches (test AND example)
        // Document 3 matches NOT(test)
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 2));
        assertTrue(results.stream().anyMatch(m -> m.documentId() == 3));
    }
} 
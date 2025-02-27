package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.ContainsCondition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.LogicalCondition;
import com.example.query.model.LogicalCondition.LogicalOperator;
import com.example.query.model.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for sentence granularity feature in query execution.
 */
public class SentenceGranularityTest {

    @Mock
    private IndexAccess mockIndexAccess;

    private QueryExecutor queryExecutor;
    private Map<String, IndexAccess> indexes;
    private PositionList positionList;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Create a ConditionExecutorFactory with real executors
        ConditionExecutorFactory factory = new ConditionExecutorFactory();
        
        // Create the QueryExecutor with the factory
        queryExecutor = new QueryExecutor(factory);
        
        // Set up indexes map with mock index
        indexes = new HashMap<>();
        indexes.put("unigram", mockIndexAccess);
        
        // Create a position list with positions in different documents and sentences
        positionList = new PositionList();
        
        // Document 1, Sentence 1
        positionList.add(new Position(1, 1, 10, 15, LocalDate.now()));
        positionList.add(new Position(1, 1, 20, 25, LocalDate.now()));
        
        // Document 1, Sentence 2
        positionList.add(new Position(1, 2, 30, 35, LocalDate.now()));
        positionList.add(new Position(1, 2, 40, 45, LocalDate.now()));
        
        // Document 2, Sentence 1
        positionList.add(new Position(2, 1, 10, 15, LocalDate.now()));
        
        // Document 3, Sentence 2
        positionList.add(new Position(3, 2, 20, 25, LocalDate.now()));
    }

    @Test
    public void testDocumentGranularity() throws Exception {
        // Set up mock to return our position list for any term
        when(mockIndexAccess.get(any())).thenReturn(Optional.of(positionList));
        
        // Create a simple CONTAINS condition
        ContainsCondition condition = new ContainsCondition(List.of("test"));
        
        // Create a query with document granularity
        Query query = new Query("test");
        query.addCondition(condition);
        
        // Execute the query with document granularity
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results
        assertEquals(3, results.size(), "Should find 3 documents");
        
        // Check that we have document-level matches
        for (DocSentenceMatch match : results) {
            assertEquals(-1, match.getSentenceId(), "Should be a document-level match");
            assertTrue(match.getDocumentId() >= 1 && match.getDocumentId() <= 3, 
                    "Document ID should be between 1 and 3");
        }
    }

    @Test
    public void testSentenceGranularity() throws Exception {
        // Set up mock to return our position list for any term
        when(mockIndexAccess.get(any())).thenReturn(Optional.of(positionList));
        
        // Create a simple CONTAINS condition
        ContainsCondition condition = new ContainsCondition(List.of("test"));
        
        // Create a query with sentence granularity
        Query query = new Query("test", List.of(condition), List.of(), 
                Optional.empty(), Optional.of(Query.Granularity.SENTENCE), Optional.empty());
        
        // Execute the query with sentence granularity
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results
        assertEquals(4, results.size(), "Should find 4 sentences");
        
        // Check that we have sentence-level matches
        int sentenceCount = 0;
        for (DocSentenceMatch match : results) {
            assertTrue(match.getSentenceId() >= 0, "Should be a sentence-level match");
            
            // Count sentences per document
            if (match.getDocumentId() == 1 && match.getSentenceId() == 1) sentenceCount++;
            if (match.getDocumentId() == 1 && match.getSentenceId() == 2) sentenceCount++;
            if (match.getDocumentId() == 2 && match.getSentenceId() == 1) sentenceCount++;
            if (match.getDocumentId() == 3 && match.getSentenceId() == 2) sentenceCount++;
        }
        
        assertEquals(4, sentenceCount, "Should find all 4 sentences");
    }

    @Test
    public void testLogicalAndWithSentenceGranularity() throws Exception {
        // Create two position lists for different terms
        PositionList positionList1 = new PositionList();
        positionList1.add(new Position(1, 1, 10, 15, LocalDate.now())); // Doc 1, Sent 1
        positionList1.add(new Position(1, 2, 30, 35, LocalDate.now())); // Doc 1, Sent 2
        positionList1.add(new Position(2, 1, 10, 15, LocalDate.now())); // Doc 2, Sent 1
        
        PositionList positionList2 = new PositionList();
        positionList2.add(new Position(1, 1, 20, 25, LocalDate.now())); // Doc 1, Sent 1
        positionList2.add(new Position(2, 2, 30, 35, LocalDate.now())); // Doc 2, Sent 2
        positionList2.add(new Position(3, 1, 10, 15, LocalDate.now())); // Doc 3, Sent 1
        
        // Set up mock to return different position lists for different terms
        when(mockIndexAccess.get("term1".getBytes())).thenReturn(Optional.of(positionList1));
        when(mockIndexAccess.get("term2".getBytes())).thenReturn(Optional.of(positionList2));
        
        // Create two CONTAINS conditions
        ContainsCondition condition1 = new ContainsCondition(List.of("term1"));
        ContainsCondition condition2 = new ContainsCondition(List.of("term2"));
        
        // Create a logical AND condition
        LogicalCondition andCondition = new LogicalCondition(
                LogicalOperator.AND, Arrays.asList(condition1, condition2));
        
        // Create a query with sentence granularity
        Query query = new Query("test", List.of(andCondition), List.of(), 
                Optional.empty(), Optional.of(Query.Granularity.SENTENCE), Optional.empty());
        
        // Execute the query
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results - only Doc 1, Sent 1 has both terms
        assertEquals(1, results.size(), "Should find 1 sentence with both terms");
        
        DocSentenceMatch match = results.iterator().next();
        assertEquals(1, match.getDocumentId(), "Should be document 1");
        assertEquals(1, match.getSentenceId(), "Should be sentence 1");
    }

    @Test
    public void testLogicalOrWithSentenceGranularity() throws Exception {
        // Create two position lists for different terms
        PositionList positionList1 = new PositionList();
        positionList1.add(new Position(1, 1, 10, 15, LocalDate.now())); // Doc 1, Sent 1
        positionList1.add(new Position(2, 1, 10, 15, LocalDate.now())); // Doc 2, Sent 1
        
        PositionList positionList2 = new PositionList();
        positionList2.add(new Position(1, 2, 20, 25, LocalDate.now())); // Doc 1, Sent 2
        positionList2.add(new Position(3, 1, 10, 15, LocalDate.now())); // Doc 3, Sent 1
        
        // Set up mock to return different position lists for different terms
        when(mockIndexAccess.get("term1".getBytes())).thenReturn(Optional.of(positionList1));
        when(mockIndexAccess.get("term2".getBytes())).thenReturn(Optional.of(positionList2));
        
        // Create two CONTAINS conditions
        ContainsCondition condition1 = new ContainsCondition(List.of("term1"));
        ContainsCondition condition2 = new ContainsCondition(List.of("term2"));
        
        // Create a logical OR condition
        LogicalCondition orCondition = new LogicalCondition(
                LogicalOperator.OR, Arrays.asList(condition1, condition2));
        
        // Create a query with sentence granularity
        Query query = new Query("test", List.of(orCondition), List.of(), 
                Optional.empty(), Optional.of(Query.Granularity.SENTENCE), Optional.empty());
        
        // Execute the query
        Set<DocSentenceMatch> results = queryExecutor.execute(query, indexes);
        
        // Verify results - should find 4 sentences with either term
        assertEquals(4, results.size(), "Should find 4 sentences with either term");
        
        // Check that we have the expected sentences
        boolean foundDoc1Sent1 = false;
        boolean foundDoc1Sent2 = false;
        boolean foundDoc2Sent1 = false;
        boolean foundDoc3Sent1 = false;
        
        for (DocSentenceMatch match : results) {
            if (match.getDocumentId() == 1 && match.getSentenceId() == 1) foundDoc1Sent1 = true;
            if (match.getDocumentId() == 1 && match.getSentenceId() == 2) foundDoc1Sent2 = true;
            if (match.getDocumentId() == 2 && match.getSentenceId() == 1) foundDoc2Sent1 = true;
            if (match.getDocumentId() == 3 && match.getSentenceId() == 1) foundDoc3Sent1 = true;
        }
        
        assertTrue(foundDoc1Sent1, "Should find Doc 1, Sent 1");
        assertTrue(foundDoc1Sent2, "Should find Doc 1, Sent 2");
        assertTrue(foundDoc2Sent1, "Should find Doc 2, Sent 1");
        assertTrue(foundDoc3Sent1, "Should find Doc 3, Sent 1");
    }
} 
package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.model.DependencyCondition;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DependencyConditionExecutorTest {

    private DependencyConditionExecutor executor;
    
    @Mock
    private IndexAccess dependencyIndex;
    
    private Map<String, IndexAccess> indexes;
    private VariableBindings variableBindings;

    @BeforeEach
    public void setUp() {
        executor = new DependencyConditionExecutor();
        indexes = new HashMap<>();
        indexes.put("dependency", dependencyIndex);
        variableBindings = new VariableBindings();
    }

    @Test
    public void testExecuteDependencySearch() throws Exception {
        // Create a dependency condition
        DependencyCondition condition = new DependencyCondition("president", "nsubj", "spoke");
        
        // Create a position list with some document positions
        PositionList positionList = new PositionList();
        LocalDate now = LocalDate.now();
        positionList.add(new Position(1, 1, 5, 6, now));
        positionList.add(new Position(2, 1, 10, 11, now));
        positionList.add(new Position(3, 1, 15, 16, now));
        
        // Mock the index response
        when(dependencyIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute the condition
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, variableBindings, Query.Granularity.DOCUMENT);
        
        // Verify the results
        assertEquals(3, result.size());
        Set<Integer> docIds = result.stream().map(DocSentenceMatch::getDocumentId).collect(Collectors.toSet());
        assertTrue(docIds.contains(1));
        assertTrue(docIds.contains(2));
        assertTrue(docIds.contains(3));
        
        // Verify the correct key was used for the search
        verify(dependencyIndex).get(argThat(keyBytes -> {
            String key = new String(keyBytes);
            // Use null byte delimiter instead of colon
            return key.equals("president" + IndexAccess.NGRAM_DELIMITER + "nsubj" + IndexAccess.NGRAM_DELIMITER + "spoke");
        }));
    }
    
    @Test
    public void testExecuteDependencyNotFound() throws Exception {
        // Create a dependency condition for a pattern that doesn't exist
        DependencyCondition condition = new DependencyCondition("nonexistent", "relation", "term");
        
        // Mock the index response for a pattern that doesn't exist
        when(dependencyIndex.get(any())).thenReturn(Optional.empty());
        
        // Execute the condition
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, variableBindings, Query.Granularity.DOCUMENT);
        
        // Verify the results are empty
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testExecuteWithMissingIndex() {
        // Create a dependency condition
        DependencyCondition condition = new DependencyCondition("governor", "relation", "dependent");
        
        // Remove the dependency index
        indexes.remove("dependency");
        
        // Execute the condition and expect an exception
        QueryExecutionException exception = assertThrows(QueryExecutionException.class, 
            () -> executor.execute(condition, indexes, variableBindings, Query.Granularity.DOCUMENT));
        
        // Verify the exception details
        assertEquals(QueryExecutionException.ErrorType.INDEX_ACCESS_ERROR, exception.getErrorType());
        assertTrue(exception.getMessage().contains("Required index not found: dependency"));
    }
} 
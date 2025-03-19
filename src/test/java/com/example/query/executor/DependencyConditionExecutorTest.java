package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.core.Position;
import com.example.core.PositionList;
import com.example.query.binding.BindingContext;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Dependency;

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

    private DependencyExecutor executor;
    
    @Mock
    private IndexAccess dependencyIndex;
    
    private Map<String, IndexAccess> indexes;
    private BindingContext bindingContext;

    @BeforeEach
    public void setUp() {
        executor = new DependencyExecutor();
        indexes = new HashMap<>();
        indexes.put("dependency", dependencyIndex);
        bindingContext = BindingContext.empty();
    }

    @Test
    public void testExecuteDependencySearch() throws Exception {
        // Create a dependency condition
        Dependency condition = new Dependency("president", "nsubj", "spoke");
        
        // Create a position list with some document positions
        PositionList positionList = new PositionList();
        LocalDate now = LocalDate.now();
        positionList.add(new Position(1, 1, 5, 6, now));
        positionList.add(new Position(2, 1, 10, 11, now));
        positionList.add(new Position(3, 1, 15, 16, now));
        
        // Mock the index response
        when(dependencyIndex.get(any())).thenReturn(Optional.of(positionList));
        
        // Execute the condition
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify the results
        assertEquals(3, result.size());
        Set<Integer> docIds = result.stream().map(DocSentenceMatch::documentId).collect(Collectors.toSet());
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
        Dependency condition = new Dependency("nonexistent", "relation", "term");
        
        // Mock the index response for a pattern that doesn't exist
        when(dependencyIndex.get(any())).thenReturn(Optional.empty());
        
        // Execute the condition
        Set<DocSentenceMatch> result = executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0);
        
        // Verify the results are empty
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testExecuteWithMissingIndex() {
        // Create a dependency condition
        Dependency condition = new Dependency("governor", "relation", "dependent");
        
        // Remove the dependency index
        indexes.remove("dependency");
        
        // Execute the condition and expect an exception
        QueryExecutionException exception = assertThrows(QueryExecutionException.class, 
            () -> executor.execute(condition, indexes, bindingContext, Query.Granularity.DOCUMENT, 0));
        
        // Verify the exception details
        assertEquals(QueryExecutionException.ErrorType.MISSING_INDEX, exception.getErrorType());
        assertTrue(exception.getMessage().contains("Missing required dependency index"));
    }
} 
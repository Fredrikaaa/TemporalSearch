package com.example.query;

import com.example.query.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("QuerySemanticValidator Tests")
class QuerySemanticValidatorTest {
    private QuerySemanticValidator validator;

    @BeforeEach
    void setUp() {
        validator = new QuerySemanticValidator();
    }

    @Test
    @DisplayName("Valid query should pass validation")
    void validQueryShouldPassValidation() throws QueryParseException {
        Query query = new Query("wikipedia", 
            Arrays.asList(
                new ContainsCondition("test"),
                new NerCondition("PERSON", "Einstein", false)
            ),
            Collections.singletonList(new OrderSpec("date", OrderSpec.Direction.DESC)),
            Optional.of(10),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }

    @Test
    @DisplayName("Empty CONTAINS value should throw exception")
    void emptyContainsValueShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.singletonList(new ContainsCondition("")),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        assertThrows(QueryParseException.class, () -> validator.validate(query));
    }

    @Test
    @DisplayName("Invalid NER type should throw exception")
    void invalidNerTypeShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.singletonList(new NerCondition("INVALID_TYPE", "test", false)),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Invalid NER type"));
    }

    @Test
    @DisplayName("Variable type mismatch should throw exception")
    void variableTypeMismatchShouldThrow() {
        Query query = new Query("wikipedia",
            Arrays.asList(
                new NerCondition("PERSON", "scientist", true),
                new NerCondition("ORGANIZATION", "scientist", true)
            ),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Type mismatch for variable"));
    }

    @Test
    @DisplayName("Invalid temporal BETWEEN dates should throw exception")
    void invalidTemporalBetweenShouldThrow() {
        LocalDateTime start = LocalDateTime.now();
        LocalDateTime end = start.minusDays(1); // End before start

        Query query = new Query("wikipedia",
            Collections.singletonList(new TemporalCondition(start, end)),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Start date must be before end date"));
    }

    @Test
    @DisplayName("Empty dependency components should throw exception")
    void emptyDependencyComponentsShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.singletonList(new DependencyCondition("", "nsubj", "cat")),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("cannot have empty components"));
    }

    @Test
    @DisplayName("Empty order by field should throw exception")
    void emptyOrderByFieldShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.emptyList(),
            Collections.singletonList(new OrderSpec("")),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Order by field cannot be empty"));
    }

    @Test
    @DisplayName("Invalid limit should throw exception")
    void invalidLimitShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.of(0),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Limit must be greater than 0"));
    }
    
    @Test
    @DisplayName("Unbound variable in ORDER BY should throw exception")
    void unboundVariableInOrderByShouldThrow() {
        Query query = new Query("wikipedia",
            Arrays.asList(
                new ContainsCondition("test")
            ),
            Collections.singletonList(new OrderSpec("?unboundVar", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Unbound variable in ORDER BY"));
    }
    
    @Test
    @DisplayName("Bound variable in ORDER BY should pass validation")
    void boundVariableInOrderByShouldPass() {
        Query query = new Query("wikipedia",
            Arrays.asList(
                new NerCondition("PERSON", "scientist", true)
            ),
            Collections.singletonList(new OrderSpec("?scientist", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Complex nested conditions should correctly track bound variables")
    void complexNestedConditionsShouldTrackBoundVariables() {
        // Create a logical condition with a NOT condition inside
        NerCondition nerCondition = new NerCondition("PERSON", "scientist", true);
        NotCondition notCondition = new NotCondition(new ContainsCondition("irrelevant"));
        LogicalCondition logicalCondition = new LogicalCondition(
            LogicalCondition.LogicalOperator.AND,
            Arrays.asList(nerCondition, notCondition)
        );
        
        Query query = new Query("wikipedia",
            Collections.singletonList(logicalCondition),
            Collections.singletonList(new OrderSpec("?scientist", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Optional.empty(),  // granularity
            Optional.empty()   // granularitySize
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    // Note: We can't directly test SnippetNode validation until the Query model is updated
    // to include select columns. The following test is commented out as a reference for
    // future implementation.
    
    /*
    @Test
    @DisplayName("Unbound variable in SNIPPET should throw exception")
    void unboundVariableInSnippetShouldThrow() {
        // Create a query with a snippet node that references an unbound variable
        SnippetNode snippetNode = new SnippetNode("?unboundVar", 1);
        
        // Once Query model is updated, we'll need to pass the select columns
        Query query = new Query("wikipedia",
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        
        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Unbound variable in SNIPPET"));
    }
    
    @Test
    @DisplayName("Excessive window size in SNIPPET should throw exception")
    void excessiveWindowSizeInSnippetShouldThrow() {
        // Create a query with a snippet node that has a window size that exceeds the maximum
        SnippetNode snippetNode = new SnippetNode("?person", 10); // Assuming MAX_SNIPPET_WINDOW_SIZE = 5
        
        // Once Query model is updated, we'll need to pass the select columns
        Query query = new Query("wikipedia",
            Collections.singletonList(new NerCondition("PERSON", "person", true)),
            Collections.emptyList(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        
        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Snippet window size"));
    }
    */
} 
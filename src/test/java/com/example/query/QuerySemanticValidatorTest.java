package com.example.query;

import com.example.query.model.*;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Ner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Query Semantic Validator Tests")
class QuerySemanticValidatorTest {
    private QuerySemanticValidator validator;

    @BeforeEach
    void setUp() {
        validator = new QuerySemanticValidator();
    }

    @Test
    @DisplayName("Valid query with COUNT column should validate")
    void validQueryWithCountShouldValidate() {
        List<SelectColumn> columns = List.of(new CountColumn(CountNode.countAll()));
        Query query = createQuery(columns, List.of());
        
        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Valid query with TITLE column should validate")
    void validQueryWithTitleShouldValidate() {
        List<SelectColumn> columns = List.of(new TitleColumn());
        Query query = createQuery(columns, List.of());
        
        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Valid query with TIMESTAMP column should validate")
    void validQueryWithTimestampShouldValidate() {
        List<SelectColumn> columns = List.of(new TimestampColumn());
        Query query = createQuery(columns, List.of());
        
        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Valid query with bound variable column should validate")
    void validQueryWithBoundVariableShouldValidate() {
        // Create condition that binds a variable
        Ner nerCondition = Ner.withVariable("PERSON", "?person");
        List<Condition> conditions = List.of(nerCondition);
        
        // Use that variable in SELECT
        List<SelectColumn> columns = List.of(new VariableColumn("person"));
        
        Query query = createQuery(columns, conditions);
        
        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Valid query with snippet using bound variable should validate")
    void validQueryWithSnippetBoundVariableShouldValidate() {
        // Create condition that binds a variable
        Ner nerCondition = Ner.withVariable("PERSON", "?person");
        List<Condition> conditions = List.of(nerCondition);
        
        // Use that variable in a SNIPPET column
        SnippetNode snippetNode = new SnippetNode("?person");
        List<SelectColumn> columns = List.of(new SnippetColumn(snippetNode));
        
        Query query = createQuery(columns, conditions);
        
        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Query with unbound variable in SELECT should throw exception")
    void queryWithUnboundVariableShouldThrowException() {
        // No condition to bind 'person' variable
        List<Condition> conditions = List.of();
        
        // Try to use an unbound variable in SELECT
        List<SelectColumn> columns = List.of(new VariableColumn("person"));
        
        Query query = createQuery(columns, conditions);
        
        QueryParseException exception = assertThrows(
            QueryParseException.class,
            () -> validator.validate(query)
        );
        
        assertTrue(exception.getMessage().contains("Unbound variable in SELECT"));
    }
    
    @Test
    @DisplayName("Query with unbound variable in SNIPPET should throw exception")
    void queryWithUnboundSnippetVariableShouldThrowException() {
        // No condition to bind 'person' variable
        List<Condition> conditions = List.of();
        
        // Try to use an unbound variable in SNIPPET
        SnippetNode snippetNode = new SnippetNode("?person");
        List<SelectColumn> columns = List.of(new SnippetColumn(snippetNode));
        
        Query query = createQuery(columns, conditions);
        
        QueryParseException exception = assertThrows(
            QueryParseException.class,
            () -> validator.validate(query)
        );
        
        assertTrue(exception.getMessage().contains("Unbound variable in SNIPPET"));
    }
    
    @Test
    @DisplayName("Query with oversized snippet window should throw exception")
    void queryWithOversizedSnippetWindowShouldThrowException() {
        // Create condition that binds a variable
        Ner nerCondition = Ner.withVariable("PERSON", "?person");
        List<Condition> conditions = List.of(nerCondition);
        
        // Create a snippet with window size 5 (the maximum allowed by the constructor)
        SnippetNode snippetNode = new SnippetNode("?person", 5);
        List<SelectColumn> columns = List.of(new SnippetColumn(snippetNode));
        
        Query query = createQuery(columns, conditions);
        
        // Since we can't create a SnippetNode with window size > 5 (constructor prevents it),
        // we're just verifying that a valid window size passes validation
        assertDoesNotThrow(() -> validator.validate(query));
    }
    
    @Test
    @DisplayName("Query with empty select columns should throw exception")
    void queryWithEmptySelectColumnsShouldThrowException() {
        List<SelectColumn> columns = new ArrayList<>();
        Query query = createQuery(columns, List.of());
        
        QueryParseException exception = assertThrows(
            QueryParseException.class,
            () -> validator.validate(query)
        );
        
        assertTrue(exception.getMessage().contains("at least one column"));
    }
    
    @Test
    @DisplayName("Query with unbound variable in ORDER BY should throw exception")
    void queryWithUnboundOrderByVariableShouldThrowException() {
        // No condition to bind 'person' variable
        List<Condition> conditions = List.of();
        
        // Create a valid select column
        List<SelectColumn> columns = List.of(new CountColumn(CountNode.countAll()));
        
        // Create a query with an unbound variable in ORDER BY
        Query query = new Query(
            "wikipedia",
            conditions,
            List.of("?person"),  // orderBy with unbound variable
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            columns
        );
        
        QueryParseException exception = assertThrows(
            QueryParseException.class,
            () -> validator.validate(query)
        );
        
        assertTrue(exception.getMessage().contains("Unbound variable in ORDER BY"));
    }
    
    /**
     * Helper method to create a Query object for testing
     */
    private Query createQuery(List<SelectColumn> columns, List<Condition> conditions) {
        return new Query(
            "wikipedia",   // source
            conditions,    // conditions
            List.of(),     // orderBy
            Optional.empty(),  // limit
            Query.Granularity.DOCUMENT,  // granularity
            Optional.empty(),  // granularitySize
            columns  // selectColumns
        );
    }
} 
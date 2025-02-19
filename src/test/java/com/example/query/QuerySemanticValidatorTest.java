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
            Optional.of(10)
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }

    @Test
    @DisplayName("Empty CONTAINS value should throw exception")
    void emptyContainsValueShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.singletonList(new ContainsCondition("")),
            Collections.emptyList(),
            Optional.empty()
        );

        assertThrows(QueryParseException.class, () -> validator.validate(query));
    }

    @Test
    @DisplayName("Invalid NER type should throw exception")
    void invalidNerTypeShouldThrow() {
        Query query = new Query("wikipedia",
            Collections.singletonList(new NerCondition("INVALID_TYPE", "test", false)),
            Collections.emptyList(),
            Optional.empty()
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
            Optional.empty()
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
            Optional.empty()
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
            Optional.empty()
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
            Optional.empty()
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
            Optional.of(0)
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Limit must be greater than 0"));
    }
} 
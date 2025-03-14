package com.example.query;

import com.example.query.model.*;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Dependency;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Ner;
import com.example.query.model.condition.Temporal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
        Query query = new Query(
            "wikipedia", 
            Arrays.asList(
                new Contains("test"),
                Ner.of("PERSON")
            ),
            Collections.singletonList(new OrderSpec("date", OrderSpec.Direction.DESC)),
            Optional.of(10),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }

    @Test
    @DisplayName("Empty CONTAINS value should throw exception")
    void emptyContainsValueShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Collections.singletonList(new Contains("")),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        assertThrows(QueryParseException.class, () -> validator.validate(query));
    }

    @Test
    @DisplayName("Invalid NER type should throw exception")
    void invalidNerTypeShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Collections.singletonList(Ner.of("INVALID_TYPE")),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        QueryParseException e = assertThrows(QueryParseException.class, 
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Invalid NER type"));
    }

    @Test
    @DisplayName("Variable type mismatch should throw exception")
    void variableTypeMismatchShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Arrays.asList(
                Ner.withVariable("PERSON", "?scientist"),
                Ner.withVariable("ORGANIZATION", "?scientist")
            ),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
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

        Query query = new Query(
            "wikipedia",
            Collections.singletonList(new Temporal(start, end)),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        QueryParseException e = assertThrows(QueryParseException.class,
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("End date must be after start date"));
    }

    @Test
    @DisplayName("Empty dependency components should throw exception")
    void emptyDependencyComponentsShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Collections.singletonList(new Dependency("", "nsubj", "object")),
            Collections.emptyList(),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        QueryParseException e = assertThrows(QueryParseException.class,
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Empty dependency component"));
    }

    @Test
    @DisplayName("Empty order by field should throw exception")
    void emptyOrderByFieldShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Collections.emptyList(),
            Collections.singletonList(new OrderSpec("", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        QueryParseException e = assertThrows(QueryParseException.class,
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Empty order by field"));
    }

    @Test
    @DisplayName("Invalid limit should throw exception")
    void invalidLimitShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.of(-1),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        QueryParseException e = assertThrows(QueryParseException.class,
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Invalid limit"));
    }

    @Test
    @DisplayName("Unbound variable in ORDER BY should throw exception")
    void unboundVariableInOrderByShouldThrow() {
        Query query = new Query(
            "wikipedia",
            Collections.singletonList(new Contains("test")),
            Collections.singletonList(new OrderSpec("?person", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        QueryParseException e = assertThrows(QueryParseException.class,
            () -> validator.validate(query));
        assertTrue(e.getMessage().contains("Unbound variable"));
    }

    @Test
    @DisplayName("Bound variable in ORDER BY should pass validation")
    void boundVariableInOrderByShouldPass() {
        Query query = new Query(
            "wikipedia",
            Collections.singletonList(Ner.withVariable("PERSON", "?person")),
            Collections.singletonList(new OrderSpec("?person", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }

    @Test
    @DisplayName("Complex nested conditions should correctly track bound variables")
    void complexNestedConditionsShouldTrackBoundVariables() {
        Query query = new Query(
            "wikipedia",
            List.of(
                new Logical(
                    Logical.LogicalOperator.AND,
                    Arrays.asList(
                        Ner.withVariable("PERSON", "?person"),
                        new Contains("scientist"),
                        new Logical(
                            Logical.LogicalOperator.OR,
                            Arrays.asList(
                                new Dependency("?person", "nsubj", "discovered"),
                                new Dependency("?person", "nsubj", "invented")
                            )
                        )
                    )
                )
            ),
            Collections.singletonList(new OrderSpec("?person", OrderSpec.Direction.ASC)),
            Optional.empty(),
            Query.Granularity.DOCUMENT,
            Optional.empty(),
            List.of()
        );

        assertDoesNotThrow(() -> validator.validate(query));
    }
} 
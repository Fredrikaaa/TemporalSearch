package com.example.query;

import com.example.query.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Query Parser Tests")
class QueryParserTest {
    private QueryParser parser;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    @BeforeEach
    void setUp() {
        parser = new QueryParser();
    }

    @Test
    @DisplayName("Parse simple query without conditions")
    void parseSimpleQuery() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia";
        Query query = parser.parse(queryStr);

        assertEquals("wikipedia", query.getSource());
        assertTrue(query.getConditions().isEmpty());
        assertTrue(query.getOrderBy().isEmpty());
        assertFalse(query.getLimit().isPresent());
    }

    @Test
    @DisplayName("Parse query with CONTAINS condition")
    void parseContainsCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE CONTAINS(\"artificial intelligence\")";
        Query query = parser.parse(queryStr);

        assertEquals("wikipedia", query.getSource());
        assertEquals(1, query.getConditions().size());

        Condition condition = query.getConditions().get(0);
        assertTrue(condition instanceof ContainsCondition);
        assertEquals("artificial intelligence", ((ContainsCondition) condition).getValue());
    }

    @Test
    @DisplayName("Parse query with NER condition")
    void parseNerCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(PERSON, \"Albert Einstein\")";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.getConditions().size());
        assertTrue(query.getConditions().get(0) instanceof NerCondition);
        
        NerCondition condition = (NerCondition) query.getConditions().get(0);
        assertEquals("PERSON", condition.getEntityType());
        assertEquals("Albert Einstein", condition.getTarget());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with NER variable condition")
    void parseNerVariableCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(PERSON, ?scientist)";
        Query query = parser.parse(queryStr);

        NerCondition condition = (NerCondition) query.getConditions().get(0);
        assertEquals("PERSON", condition.getEntityType());
        assertEquals("scientist", condition.getTarget());
        assertTrue(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with temporal condition")
    void parseTemporalCondition() throws QueryParseException {
        String date = "2024-01-01T00:00:00";
        String queryStr = "SELECT documents FROM wikipedia WHERE TEMPORAL(AFTER \"" + date + "\")";
        Query query = parser.parse(queryStr);

        assertTrue(query.getConditions().get(0) instanceof TemporalCondition);
        TemporalCondition condition = (TemporalCondition) query.getConditions().get(0);
        
        assertEquals(TemporalCondition.Type.AFTER, condition.getTemporalType());
        assertEquals(LocalDateTime.parse(date, DATE_FORMATTER), condition.getStartDate());
    }

    @Test
    @DisplayName("Parse query with temporal between condition")
    void parseTemporalBetweenCondition() throws QueryParseException {
        String start = "2024-01-01T00:00:00";
        String end = "2024-12-31T23:59:59";
        String queryStr = String.format(
            "SELECT documents FROM wikipedia WHERE TEMPORAL(BETWEEN \"%s\" AND \"%s\")",
            start, end
        );
        Query query = parser.parse(queryStr);

        TemporalCondition condition = (TemporalCondition) query.getConditions().get(0);
        assertEquals(TemporalCondition.Type.BETWEEN, condition.getTemporalType());
        assertEquals(LocalDateTime.parse(start, DATE_FORMATTER), condition.getStartDate());
        assertTrue(condition.getEndDate().isPresent());
        assertEquals(LocalDateTime.parse(end, DATE_FORMATTER), condition.getEndDate().get());
    }

    @Test
    @DisplayName("Parse query with dependency condition")
    void parseDependencyCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DEPENDENCY(\"eat\", \"nsubj\", \"cat\")";
        Query query = parser.parse(queryStr);

        assertTrue(query.getConditions().get(0) instanceof DependencyCondition);
        DependencyCondition condition = (DependencyCondition) query.getConditions().get(0);
        
        assertEquals("eat", condition.getGovernor());
        assertEquals("nsubj", condition.getRelation());
        assertEquals("cat", condition.getDependent());
    }

    @Test
    @DisplayName("Parse query with multiple conditions")
    void parseMultipleConditions() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE CONTAINS(\"physics\") " +
                         "NER(PERSON, \"Einstein\")";
        Query query = parser.parse(queryStr);

        assertEquals(2, query.getConditions().size());
        assertTrue(query.getConditions().get(0) instanceof ContainsCondition);
        assertTrue(query.getConditions().get(1) instanceof NerCondition);
    }

    @Test
    @DisplayName("Parse query with order by clause")
    void parseOrderByClause() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia ORDER BY date DESC, relevance ASC";
        Query query = parser.parse(queryStr);

        assertEquals(2, query.getOrderBy().size());
        assertEquals("date", query.getOrderBy().get(0).getField());
        assertEquals(OrderSpec.Direction.DESC, query.getOrderBy().get(0).getDirection());
        assertEquals("relevance", query.getOrderBy().get(1).getField());
        assertEquals(OrderSpec.Direction.ASC, query.getOrderBy().get(1).getDirection());
    }

    @Test
    @DisplayName("Parse query with limit clause")
    void parseLimitClause() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia LIMIT 10";
        Query query = parser.parse(queryStr);

        assertTrue(query.getLimit().isPresent());
        assertEquals(10, query.getLimit().get());
    }

    @Test
    @DisplayName("Parse complex query with all features")
    void parseComplexQuery() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE CONTAINS(\"quantum physics\") " +
                         "NER(PERSON, \"Bohr\") " +
                         "TEMPORAL(AFTER \"2000-01-01T00:00:00\") " +
                         "ORDER BY date DESC " +
                         "LIMIT 5";
        Query query = parser.parse(queryStr);

        assertEquals("wikipedia", query.getSource());
        assertEquals(3, query.getConditions().size());
        assertEquals(1, query.getOrderBy().size());
        assertEquals(5, query.getLimit().get());
    }

    @Test
    @DisplayName("Parse query with nested conditions")
    void parseNestedConditions() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE (CONTAINS(\"physics\") NER(PERSON, \"Einstein\"))";
        Query query = parser.parse(queryStr);

        assertEquals(2, query.getConditions().size());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT documents wikipedia",  // Missing FROM
        "SELECT documents FROM",       // Missing source
        "SELECT documents FROM wikipedia WHERE",  // Incomplete WHERE clause
        "SELECT documents FROM wikipedia ORDER",  // Incomplete ORDER BY
        "SELECT documents FROM wikipedia LIMIT"   // Missing limit value
    })
    @DisplayName("Parse invalid queries should throw exception")
    void parseInvalidQueriesShouldThrowException(String queryStr) {
        assertThrows(QueryParseException.class, () -> parser.parse(queryStr));
    }
} 
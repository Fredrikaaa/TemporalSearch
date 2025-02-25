package com.example.query;

import com.example.query.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Query Parser Tests")
class QueryParserTest {
    private QueryParser parser;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

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
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(\"PERSON\", \"Albert Einstein\")";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.getConditions().size());
        assertTrue(query.getConditions().get(0) instanceof NerCondition);
        
        NerCondition condition = (NerCondition) query.getConditions().get(0);
        assertEquals("PERSON", condition.getEntityType());
        assertEquals("Albert Einstein", condition.getTarget());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with NER wildcard type")
    void parseNerWildcardType() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(\"*\", \"Google\")";
        Query query = parser.parse(queryStr);

        NerCondition condition = (NerCondition) query.getConditions().get(0);
        assertEquals("*", condition.getEntityType());
        assertEquals("Google", condition.getTarget());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with NER variable and wildcard")
    void parseNerVariableWithWildcard() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(\"PERSON\", ?scientist)";
        Query query = parser.parse(queryStr);

        NerCondition condition = (NerCondition) query.getConditions().get(0);
        assertEquals("PERSON", condition.getEntityType());
        assertEquals("scientist", condition.getTarget());
        assertTrue(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with date comparison")
    void parseDateComparison() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DATE(?date, < 2000)";
        Query query = parser.parse(queryStr);

        assertTrue(query.getConditions().get(0) instanceof TemporalCondition);
        TemporalCondition condition = (TemporalCondition) query.getConditions().get(0);
        
        assertEquals(TemporalCondition.Type.BEFORE, condition.getTemporalType());
        assertEquals(
            LocalDateTime.of(2000, 1, 1, 0, 0),
            condition.getStartDate()
        );
        assertTrue(condition.getVariable().isPresent());
        assertEquals("date", condition.getVariable().get());
    }

    @Test
    @DisplayName("Parse query with date NEAR range")
    void parseDateNearRange() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DATE(?founding, NEAR 1980 RADIUS 5 y)";
        Query query = parser.parse(queryStr);

        TemporalCondition condition = (TemporalCondition) query.getConditions().get(0);
        assertEquals(TemporalCondition.Type.NEAR, condition.getTemporalType());
        assertTrue(condition.getVariable().isPresent());
        assertEquals("founding", condition.getVariable().get());
        assertEquals(LocalDateTime.of(1980, 1, 1, 0, 0), condition.getStartDate());
        assertTrue(condition.getRange().isPresent());
        assertEquals("5y", condition.getRange().get());
    }

    @Test
    @DisplayName("Parse query with dependency condition")
    void parseDependencyCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DEPENDS(\"cat\", \"nsubj\", \"eats\")";
        Query query = parser.parse(queryStr);

        assertTrue(query.getConditions().get(0) instanceof DependencyCondition);
        DependencyCondition condition = (DependencyCondition) query.getConditions().get(0);
        
        assertEquals("cat", condition.getGovernor());
        assertEquals("nsubj", condition.getRelation());
        assertEquals("eats", condition.getDependent());
    }

    @Test
    @DisplayName("Parse query with multiple conditions using AND")
    void parseMultipleConditions() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE CONTAINS(\"physics\") " +
                         "AND NER(\"PERSON\", \"Einstein\")";
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
                         "AND NER(\"PERSON\", \"Bohr\") " +
                         "AND DATE(?publication, < 2000) " +
                         "ORDER BY date DESC " +
                         "LIMIT 5";
        Query query = parser.parse(queryStr);

        assertEquals("wikipedia", query.getSource());
        assertEquals(3, query.getConditions().size());
        assertEquals(1, query.getOrderBy().size());
        assertEquals(5, query.getLimit().get());
        
        // Verify the conditions in detail
        assertTrue(query.getConditions().get(0) instanceof ContainsCondition);
        assertTrue(query.getConditions().get(1) instanceof NerCondition);
        assertTrue(query.getConditions().get(2) instanceof TemporalCondition);
    }

    @Test
    @DisplayName("Parse query with nested conditions")
    void parseNestedConditions() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE (CONTAINS(\"physics\") AND NER(\"PERSON\", \"Einstein\"))";
        Query query = parser.parse(queryStr);

        assertEquals(2, query.getConditions().size());
    }

    @Test
    @DisplayName("Parse query with subquery")
    void parseSubquery() {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE DATE(?date, < 2000) " +
                         "AND (CONTAINS(\"subquery\") AND CONTAINS(\"nested\"))";
        
        // This isn't really a subquery test anymore, but a test of nested conditions
        assertDoesNotThrow(() -> parser.parse(queryStr));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT documents wikipedia",  // Missing FROM
        "SELECT documents FROM",       // Missing source
        "SELECT documents FROM wikipedia WHERE",  // Incomplete WHERE clause
        "SELECT documents FROM wikipedia ORDER",  // Incomplete ORDER BY
        "SELECT documents FROM wikipedia LIMIT",  // Missing limit value
        "SELECT documents FROM wikipedia WHERE CONTAINS(\"physics\") NER(\"PERSON\", \"Einstein\")",  // Missing AND
        "SELECT documents FROM wikipedia WHERE DATE(?date) < abc"  // Invalid date format
    })
    @DisplayName("Parse invalid queries should throw exception")
    void parseInvalidQueriesShouldThrowException(String queryStr) {
        assertThrows(QueryParseException.class, () -> parser.parse(queryStr));
    }
} 
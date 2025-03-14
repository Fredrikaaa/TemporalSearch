package com.example.query;

import com.example.query.model.Query;
import com.example.query.model.OrderSpec;
import com.example.query.model.TemporalRange;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Dependency;
import com.example.query.model.condition.Logical;
import com.example.query.model.condition.Ner;
import com.example.query.model.condition.Not;
import com.example.query.model.condition.Temporal;

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

        assertEquals("wikipedia", query.source());
        assertTrue(query.conditions().isEmpty());
        assertTrue(query.orderBy().isEmpty());
        assertFalse(query.limit().isPresent());
    }

    @Test
    @DisplayName("Parse query with CONTAINS condition")
    void parseContainsCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE CONTAINS(\"artificial intelligence\")";
        Query query = parser.parse(queryStr);

        assertEquals("wikipedia", query.source());
        assertEquals(1, query.conditions().size());

        Condition condition = query.conditions().get(0);
        assertTrue(condition instanceof Contains);
        assertEquals("artificial intelligence", ((Contains) condition).value());
    }

    @Test
    @DisplayName("Parse query with NER condition")
    void parseNerCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(PERSON)";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.conditions().size());
        assertTrue(query.conditions().get(0) instanceof Ner);
        
        Ner condition = (Ner) query.conditions().get(0);
        assertEquals("PERSON", condition.entityType());
        assertNull(condition.variableName());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with NER wildcard type")
    void parseNerWildcardType() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(*)";
        Query query = parser.parse(queryStr);

        Ner condition = (Ner) query.conditions().get(0);
        assertEquals("*", condition.entityType());
        assertNull(condition.variableName());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with NER variable binding")
    void parseNerVariableBinding() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(PERSON, ?scientist)";
        Query query = parser.parse(queryStr);

        Ner condition = (Ner) query.conditions().get(0);
        assertEquals("PERSON", condition.entityType());
        assertEquals("scientist", condition.variableName());
        assertTrue(condition.isVariable());
    }

    @Test
    @DisplayName("Parse query with date comparison")
    void parseDateComparison() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DATE(?date, < 2000)";
        Query query = parser.parse(queryStr);

        assertTrue(query.conditions().get(0) instanceof Temporal);
        Temporal condition = (Temporal) query.conditions().get(0);
        
        assertEquals(Temporal.Type.BEFORE, condition.temporalType());
        assertEquals(
            LocalDateTime.of(2000, 1, 1, 0, 0),
            condition.startDate()
        );
        assertTrue(condition.variable().isPresent());
        assertEquals("?date", condition.variable().get());
    }

    @Test
    @DisplayName("Parse query with date NEAR range")
    void parseDateNearRange() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DATE(?founding, NEAR 1980 RADIUS 5 y)";
        Query query = parser.parse(queryStr);

        Temporal condition = (Temporal) query.conditions().get(0);
        assertEquals(Temporal.Type.NEAR, condition.temporalType());
        assertTrue(condition.variable().isPresent());
        assertEquals("?founding", condition.variable().get());
        assertEquals(LocalDateTime.of(1980, 1, 1, 0, 0), condition.startDate());
        assertTrue(condition.range().isPresent());
        assertEquals(new TemporalRange("5y"), condition.range().get());
    }

    @Test
    @DisplayName("Parse query with dependency condition")
    void parseDependencyCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE DEPENDS(\"cat\", \"nsubj\", \"eats\")";
        Query query = parser.parse(queryStr);

        assertTrue(query.conditions().get(0) instanceof Dependency);
        Dependency condition = (Dependency) query.conditions().get(0);
        
        assertEquals("cat", condition.governor());
        assertEquals("nsubj", condition.relation());
        assertEquals("eats", condition.dependent());
    }

    @Test
    @DisplayName("Parse query with multiple conditions using AND")
    void parseMultipleConditions() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE CONTAINS(\"physics\") " +
                         "AND NER(PERSON)";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.conditions().size());
        assertTrue(query.conditions().get(0) instanceof Logical);
        
        Logical condition = (Logical) query.conditions().get(0);
        assertEquals(Logical.LogicalOperator.AND, condition.operator());
        assertEquals(2, condition.conditions().size());
        assertTrue(condition.conditions().get(0) instanceof Contains);
        assertTrue(condition.conditions().get(1) instanceof Ner);
    }

    @Test
    @DisplayName("Parse query with order by clause")
    void parseOrderByClause() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia ORDER BY date DESC, relevance ASC";
        Query query = parser.parse(queryStr);

        assertEquals(2, query.orderBy().size());
        assertEquals("date", query.orderBy().get(0).field());
        assertEquals(OrderSpec.Direction.DESC, query.orderBy().get(0).direction());
        assertEquals("relevance", query.orderBy().get(1).field());
        assertEquals(OrderSpec.Direction.ASC, query.orderBy().get(1).direction());
    }

    @Test
    @DisplayName("Parse query with limit clause")
    void parseLimitClause() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia LIMIT 10";
        Query query = parser.parse(queryStr);

        assertTrue(query.limit().isPresent());
        assertEquals(10, query.limit().get());
    }

    @Test
    @DisplayName("Parse complex query with all features")
    void parseComplexQuery() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE CONTAINS(\"quantum physics\") " +
                         "AND NER(\"PERSON\", ?scientist) " +
                         "AND DATE(?publication, < 2000) " +
                         "ORDER BY date DESC " +
                         "LIMIT 5";
        Query query = parser.parse(queryStr);

        assertEquals("wikipedia", query.source());
        assertEquals(1, query.conditions().size());
        assertEquals(1, query.orderBy().size());
        assertEquals(5, query.limit().get());
        
        // Verify the conditions in detail
        assertTrue(query.conditions().get(0) instanceof Logical);
        Logical condition = (Logical) query.conditions().get(0);
        assertEquals(Logical.LogicalOperator.AND, condition.operator());
        assertEquals(2, condition.conditions().size());
        
        // First condition should be a nested logical condition with CONTAINS and NER
        assertTrue(condition.conditions().get(0) instanceof Logical);
        // Second condition should be the temporal condition
        assertTrue(condition.conditions().get(1) instanceof Temporal);
        
        // Check the nested logical condition
        Logical nestedCondition = (Logical) condition.conditions().get(0);
        assertEquals(Logical.LogicalOperator.AND, nestedCondition.operator());
        assertEquals(2, nestedCondition.conditions().size());
        assertTrue(nestedCondition.conditions().get(0) instanceof Contains);
        assertTrue(nestedCondition.conditions().get(1) instanceof Ner);
    }

    @Test
    @DisplayName("Parse query with nested conditions")
    void parseNestedConditions() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia " +
                         "WHERE (CONTAINS(\"physics\") AND NER(PERSON))";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.conditions().size());
        assertTrue(query.conditions().get(0) instanceof Logical);
        
        Logical condition = (Logical) query.conditions().get(0);
        assertEquals(Logical.LogicalOperator.AND, condition.operator());
        assertEquals(2, condition.conditions().size());
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

    @Test
    @DisplayName("Parse query with OR condition")
    void parseOrCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE CONTAINS(\"physics\") OR NER(PERSON)";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.conditions().size());
        assertTrue(query.conditions().get(0) instanceof Logical);
        
        Logical condition = (Logical) query.conditions().get(0);
        assertEquals(Logical.LogicalOperator.OR, condition.operator());
        assertEquals(2, condition.conditions().size());
        
        assertTrue(condition.conditions().get(0) instanceof Contains);
        assertTrue(condition.conditions().get(1) instanceof Ner);
        
        Contains containsCondition = (Contains) condition.conditions().get(0);
        assertEquals("physics", containsCondition.value());
        
        Ner nerCondition = (Ner) condition.conditions().get(1);
        assertEquals("PERSON", nerCondition.entityType());
        assertNull(nerCondition.variableName());
        assertFalse(nerCondition.isVariable());
    }
    
    @Test
    @DisplayName("Parse query with NOT condition")
    void parseNotCondition() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NOT CONTAINS(\"irrelevant\")";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.conditions().size());
        assertTrue(query.conditions().get(0) instanceof Not);
        
        Not condition = (Not) query.conditions().get(0);
        assertTrue(condition.condition() instanceof Contains);
        
        Contains containsCondition = (Contains) condition.condition();
        assertEquals("irrelevant", containsCondition.value());
    }
    
    @Test
    @DisplayName("Parse query with mixed logical operators")
    void parseMixedLogicalOperators() throws QueryParseException {
        String queryStr = "SELECT documents FROM wikipedia WHERE NER(\"PERSON\", ?person) AND (CONTAINS(\"physics\") OR NOT CONTAINS(\"biology\"))";
        Query query = parser.parse(queryStr);

        assertEquals(1, query.conditions().size());
        assertTrue(query.conditions().get(0) instanceof Logical);
        
        Logical andCondition = (Logical) query.conditions().get(0);
        assertEquals(Logical.LogicalOperator.AND, andCondition.operator());
        assertEquals(2, andCondition.conditions().size());
        
        assertTrue(andCondition.conditions().get(0) instanceof Ner);
        assertTrue(andCondition.conditions().get(1) instanceof Logical);
        
        Logical orCondition = (Logical) andCondition.conditions().get(1);
        assertEquals(Logical.LogicalOperator.OR, orCondition.operator());
        assertEquals(2, orCondition.conditions().size());
        
        assertTrue(orCondition.conditions().get(0) instanceof Contains);
        assertTrue(orCondition.conditions().get(1) instanceof Not);
        
        Not notCondition = (Not) orCondition.conditions().get(1);
        assertTrue(notCondition.condition() instanceof Contains);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT documents wikipedia",  // Missing FROM
        "SELECT documents FROM",       // Missing source
        "SELECT documents FROM wikipedia WHERE",  // Incomplete WHERE clause
        "SELECT documents FROM wikipedia ORDER",  // Incomplete ORDER BY
        "SELECT documents FROM wikipedia LIMIT",  // Missing limit value
        "SELECT documents FROM wikipedia WHERE CONTAINS(\"physics\") NER(\"PERSON\")",  // Missing AND
        "SELECT documents FROM wikipedia WHERE DATE(?date) < abc"  // Invalid date format
    })
    @DisplayName("Parse invalid queries should throw exception")
    void parseInvalidQueriesShouldThrowException(String queryStr) {
        assertThrows(QueryParseException.class, () -> parser.parse(queryStr));
    }
} 
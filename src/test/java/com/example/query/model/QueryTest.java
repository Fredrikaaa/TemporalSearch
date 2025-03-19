package com.example.query.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.query.binding.VariableRegistry;
import com.example.query.binding.VariableType;
import com.example.query.model.condition.Condition;
import com.example.query.model.condition.Contains;
import com.example.query.model.condition.Ner;

/**
 * Tests for the Query class, focusing on variable binding functionality.
 */
public class QueryTest {

    private Query query;

    @BeforeEach
    public void setUp() {
        query = new Query("test_source");
    }

    @Test
    public void testInitialState() {
        // A fresh query should have an empty variable registry
        assertTrue(query.getAllVariableNames().isEmpty());
        
        // Default parameters should be set correctly
        assertEquals("test_source", query.source());
        assertTrue(query.conditions().isEmpty());
        assertTrue(query.orderBy().isEmpty());
        assertEquals(Optional.empty(), query.limit());
        assertEquals(Query.Granularity.DOCUMENT, query.granularity());
        assertEquals(Optional.empty(), query.granularitySize());
        assertTrue(query.selectColumns().isEmpty());
        assertNotNull(query.variableRegistry());
    }

    @Test
    public void testRegisterProducer() {
        // Register a producer variable
        query.registerProducer("person", VariableType.ENTITY, "NER");
        
        // Verify registration
        assertTrue(query.isVariableProduced("person"));
        assertEquals(VariableType.ENTITY, query.getVariableType("person"));
        assertEquals(Set.of("?person"), query.getAllVariableNames());
    }

    @Test
    public void testRegisterConsumer() {
        // Register a consumer variable
        query.registerConsumer("person", VariableType.ENTITY, "CONTAINS");
        
        // Verify registration
        assertFalse(query.isVariableProduced("person"));
        assertEquals(VariableType.ENTITY, query.getVariableType("person"));
        assertEquals(Set.of("?person"), query.getAllVariableNames());
    }

    @Test
    public void testRegisterBoth() {
        // Register both producer and consumer
        query.registerProducer("person", VariableType.ENTITY, "NER");
        query.registerConsumer("person", VariableType.ENTITY, "CONTAINS");
        
        // Verify registration
        assertTrue(query.isVariableProduced("person"));
        assertEquals(VariableType.ENTITY, query.getVariableType("person"));
        assertEquals(Set.of("?person"), query.getAllVariableNames());
    }

    @Test
    public void testVariableValidation() {
        // Register just a consumer
        query.registerConsumer("person", VariableType.ENTITY, "CONTAINS");
        
        // Should fail validation since the variable is consumed but not produced
        Set<String> errors = query.validateVariables();
        assertFalse(errors.isEmpty());
        assertTrue(errors.iterator().next().contains("is consumed but never produced"));
        
        // Fix by registering a producer
        query.registerProducer("person", VariableType.ENTITY, "NER");
        errors = query.validateVariables();
        assertTrue(errors.isEmpty());
    }

    @Test
    public void testVariableTypeInference() {
        // Register with different types
        query.registerProducer("mixed", VariableType.TEXT_SPAN, "SPAN");
        query.registerConsumer("mixed", VariableType.ANY, "USE");
        
        // Should infer the more specific type
        assertEquals(VariableType.TEXT_SPAN, query.getVariableType("mixed"));
        
        // Add conflicting type
        query.registerConsumer("mixed", VariableType.ENTITY, "OTHER");
        
        // Should now use ANY due to conflict
        assertEquals(VariableType.ANY, query.getVariableType("mixed"));
    }

    @Test
    public void testMultipleVariables() {
        // Register multiple variables
        query.registerProducer("person", VariableType.ENTITY, "NER");
        query.registerProducer("org", VariableType.ENTITY, "NER");
        query.registerProducer("date", VariableType.TEMPORAL, "DATE");
        
        // Verify all are registered
        assertEquals(3, query.getAllVariableNames().size());
        assertTrue(query.getAllVariableNames().contains("?person"));
        assertTrue(query.getAllVariableNames().contains("?org"));
        assertTrue(query.getAllVariableNames().contains("?date"));
    }

    @Test
    public void testNerConditionWithVariableBinding() {
        // Create a NER condition that produces a variable
        Ner nerCondition = Ner.withVariable("PERSON", "?person");
        
        // Create a query with this condition
        Query queryWithNer = new Query("wikipedia", List.of(nerCondition));
        
        // The condition should automatically register its variable
        nerCondition.registerVariables(queryWithNer.variableRegistry());
        
        // Check variable registration
        assertTrue(queryWithNer.isVariableProduced("person"));
        assertEquals(VariableType.ENTITY, queryWithNer.getVariableType("person"));
        
        // Check that the condition properly appears in toString
        String queryString = queryWithNer.toString();
        assertTrue(queryString.contains("NER(PERSON) AS ?person"));
    }

    @Test
    public void testContainsConditionWithVariableBinding() {
        // Create a Contains condition that produces a variable
        Contains containsCondition = new Contains("search term", "result", true);
        
        // Create a query with this condition
        Query queryWithContains = new Query("news_corpus", List.of(containsCondition));
        
        // The condition should automatically register its variable
        containsCondition.registerVariables(queryWithContains.variableRegistry());
        
        // Check variable registration
        assertTrue(queryWithContains.isVariableProduced("result"));
        assertEquals(VariableType.TEXT_SPAN, queryWithContains.getVariableType("result"));
        
        // Check that the condition properly appears in toString
        String queryString = queryWithContains.toString();
        assertTrue(queryString.contains("CONTAINS(search term) AS ?result"));
    }

    @Test
    public void testComplexQueryWithMultipleConditions() {
        // Create several conditions
        Ner personCondition = Ner.withVariable("PERSON", "?person");
        Ner orgCondition = Ner.withVariable("ORGANIZATION", "?org");
        Contains containsCondition = new Contains("meeting", "text", true);
        
        // Build a query with these conditions
        List<Condition> conditions = new ArrayList<>();
        conditions.add(personCondition);
        conditions.add(orgCondition);
        conditions.add(containsCondition);
        
        Query complexQuery = new Query("news_corpus", conditions);
        
        // Register variables
        for (Condition condition : conditions) {
            condition.registerVariables(complexQuery.variableRegistry());
        }
        
        // Check all variables are properly registered
        Set<String> variables = complexQuery.getAllVariableNames();
        assertEquals(3, variables.size());
        assertTrue(variables.contains("?person"));
        assertTrue(variables.contains("?org"));
        assertTrue(variables.contains("?text"));
        
        // Check variable types
        assertEquals(VariableType.ENTITY, complexQuery.getVariableType("person"));
        assertEquals(VariableType.ENTITY, complexQuery.getVariableType("org"));
        assertEquals(VariableType.TEXT_SPAN, complexQuery.getVariableType("text"));
        
        // Validate variables (all should be produced)
        assertTrue(complexQuery.validateVariables().isEmpty());
        
        // Verify query string representation
        String queryString = complexQuery.toString();
        assertTrue(queryString.contains("FROM news_corpus"));
        assertTrue(queryString.contains("WHERE"));
        assertTrue(queryString.contains("NER(PERSON) AS ?person"));
        assertTrue(queryString.contains("NER(ORGANIZATION) AS ?org"));
        assertTrue(queryString.contains("CONTAINS(meeting) AS ?text"));
    }
} 
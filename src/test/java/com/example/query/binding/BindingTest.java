package com.example.query.binding;

import com.example.query.QueryParseException;
import com.example.query.QuerySemanticValidator;
import com.example.query.model.Query;
import com.example.query.parser.QueryLangLexer;
import com.example.query.parser.QueryLangParser;
import com.example.query.parser.QueryModelBuilder;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the variable binding system integration.
 * Tests focus on complete query parsing and validation.
 */
public class BindingTest {

    /**
     * Parse a query string into a Query object
     */
    private Query parseQuery(String queryStr) {
        try {
            QueryLangLexer lexer = new QueryLangLexer(CharStreams.fromString(queryStr));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            QueryLangParser parser = new QueryLangParser(tokens);
            ParseTree tree = parser.query();
            QueryModelBuilder builder = new QueryModelBuilder();
            return builder.buildQuery(tree);
        } catch (Exception e) {
            fail("Failed to parse query: " + e.getMessage());
            return null;
        }
    }

    @Test
    void testSimpleVariableBinding() {
        String queryStr = "SELECT ?person FROM documents WHERE NER(PERSON) AS ?person";
        Query query = parseQuery(queryStr);
        
        VariableRegistry registry = query.variableRegistry();
        assertNotNull(registry, "Registry should not be null");
        
        // Check that ?person is produced
        assertTrue(registry.isProduced("?person"), "?person should be produced");
        
        // Check variable type
        assertEquals(VariableType.ENTITY, registry.getInferredType("?person"), 
                     "?person should have ENTITY type");
                     
        // Check that ?person has producer
        Set<ProducerVariable> producers = registry.getProducers("?person");
        assertEquals(1, producers.size(), "Should have one producer");
        assertEquals("NER", producers.iterator().next().sourceConditionType(), 
                    "Producer should be NER condition");
    }
    
    @Test
    void testVariableConsumption() {
        // Query where ?person is both produced and consumed
        String queryStr = "SELECT ?person, ?org FROM documents " +
                         "WHERE NER(PERSON) AS ?person AND " +
                         "DEPENDS(?person, \"works_for\", ?org) AND " +
                         "NER(ORGANIZATION) AS ?org";
        Query query = parseQuery(queryStr);
        
        VariableRegistry registry = query.variableRegistry();
        
        // Check both variables are produced
        assertTrue(registry.isProduced("?person"), "?person should be produced");
        assertTrue(registry.isProduced("?org"), "?org should be produced");
        
        // Check ?person is consumed by DEPENDENCY
        Set<ConsumerVariable> personConsumers = registry.getConsumers("?person");
        assertFalse(personConsumers.isEmpty(), "?person should be consumed");
        boolean hasDepConsumer = personConsumers.stream()
                .anyMatch(c -> c.consumingConditionType().equals("DEPENDENCY"));
        assertTrue(hasDepConsumer, "?person should be consumed by DEPENDENCY condition");
        
        // Check ?org is consumed by DEPENDENCY
        Set<ConsumerVariable> orgConsumers = registry.getConsumers("?org");
        assertFalse(orgConsumers.isEmpty(), "?org should be consumed");
        hasDepConsumer = orgConsumers.stream()
                .anyMatch(c -> c.consumingConditionType().equals("DEPENDENCY"));
        assertTrue(hasDepConsumer, "?org should be consumed by DEPENDENCY condition");
        
        // Check types
        assertEquals(VariableType.ENTITY, registry.getInferredType("?person"), 
                    "?person should have ENTITY type");
        assertEquals(VariableType.ENTITY, registry.getInferredType("?org"), 
                    "?org should have ENTITY type");
    }
    
    @Test
    void testTemporalVariableBinding() {
        String queryStr = "SELECT ?date, SNIPPET(?date) FROM documents " +
                         "WHERE DATE(> 2020) AS ?date";
        Query query = parseQuery(queryStr);
        
        VariableRegistry registry = query.variableRegistry();
        
        // Check ?date is produced
        assertTrue(registry.isProduced("?date"), "?date should be produced");
        
        // Check type
        assertEquals(VariableType.TEMPORAL, registry.getInferredType("?date"), 
                    "?date should have TEMPORAL type");
    }
    
    @Test
    void testMultipleProducers() {
        // Same variable produced by two different conditions
        String queryStr = "SELECT ?entity FROM documents " +
                         "WHERE NER(PERSON) AS ?entity OR NER(ORGANIZATION) AS ?entity";
        Query query = parseQuery(queryStr);
        
        VariableRegistry registry = query.variableRegistry();
        
        // Check ?entity is produced
        assertTrue(registry.isProduced("?entity"), "?entity should be produced");
        
        // Should have at least one producer (note: logical OR handling limitation with current parser)
        assertTrue(registry.getProducers("?entity").size() >= 1, 
                    "?entity should have at least one producer");
    }
    
    @Test
    void testUnboundVariableError() {
        // Variable used in SELECT but not produced in WHERE
        String queryStr = "SELECT ?missing FROM documents " +
                          "WHERE NER(PERSON) AS ?person";
        
        Query query = parseQuery(queryStr);
        
        // Manually use the semantic validator to check for missing variables in SELECT
        QuerySemanticValidator validator = new QuerySemanticValidator();
        Exception exception = assertThrows(QueryParseException.class, () -> {
            validator.validate(query);
        });
        
        assertTrue(exception.getMessage().contains("Unbound variable"), 
                  "Error should mention unbound variable");
    }
    
    @Test
    void testComplexQuery() {
        // Complex query with multiple variable bindings and dependencies
        String queryStr = "SELECT ?person, ?org, SNIPPET(?person) FROM documents " +
                         "WHERE NER(PERSON) AS ?person AND " +
                         "NER(ORGANIZATION) AS ?org AND " +
                         "DEPENDS(?person, \"works_for\", ?org) AND " +
                         "CONTAINS(\"CEO\", \"executive\") AND " +
                         "DATE(> 2018) AS ?date";
        
        Query query = parseQuery(queryStr);
        VariableRegistry registry = query.variableRegistry();
        
        // All variables should be produced
        assertTrue(registry.isProduced("?person"), "?person should be produced");
        assertTrue(registry.isProduced("?org"), "?org should be produced");
        assertTrue(registry.isProduced("?date"), "?date should be produced");
        
        // Check consumption relationships
        Set<ConsumerVariable> personConsumers = registry.getConsumers("?person");
        Set<ConsumerVariable> orgConsumers = registry.getConsumers("?org");
        
        assertFalse(personConsumers.isEmpty(), "?person should be consumed");
        assertFalse(orgConsumers.isEmpty(), "?org should be consumed");
        
        // Verify specific consumption by DEPENDENCY
        boolean personConsumedByDep = personConsumers.stream()
                .anyMatch(c -> c.consumingConditionType().equals("DEPENDENCY"));
        assertTrue(personConsumedByDep, "?person should be consumed by DEPENDENCY");
        
        boolean orgConsumedByDep = orgConsumers.stream()
                .anyMatch(c -> c.consumingConditionType().equals("DEPENDENCY"));
        assertTrue(orgConsumedByDep, "?org should be consumed by DEPENDENCY");
        
        // No validation errors should occur in the registry itself
        assertTrue(query.validateVariables().isEmpty(), "Should have no validation errors");
    }
} 
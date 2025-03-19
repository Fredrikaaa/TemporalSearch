package com.example.query.binding;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collection;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the VariableRegistry class.
 */
public class VariableRegistryTest {

    private VariableRegistry registry;

    @BeforeEach
    public void setUp() {
        registry = new VariableRegistry();
    }

    @Test
    public void testRegisterProducer() {
        String variableName = "person";
        ProducerVariable var = registry.registerProducer(variableName, VariableType.ENTITY, "NER");
        
        assertEquals("?person", var.getName());
        assertEquals(VariableType.ENTITY, var.getType());
        assertEquals("NER", var.sourceConditionType());
        assertEquals(Set.of("NER"), var.producedBy());
        
        // Verify registry state
        assertTrue(registry.isProduced(variableName));
        assertEquals(1, registry.getProducers(variableName).size());
        assertEquals(0, registry.getConsumers(variableName).size());
    }

    @Test
    public void testRegisterConsumer() {
        String variableName = "person";
        ConsumerVariable var = registry.registerConsumer(variableName, VariableType.ENTITY, "CONTAINS");
        
        assertEquals("?person", var.getName());
        assertEquals(VariableType.ENTITY, var.getType());
        assertEquals("CONTAINS", var.consumingConditionType());
        assertEquals(Set.of("CONTAINS"), var.consumedBy());
        
        // Verify registry state
        assertFalse(registry.isProduced(variableName));
        assertEquals(0, registry.getProducers(variableName).size());
        assertEquals(1, registry.getConsumers(variableName).size());
    }

    @Test
    public void testRegisterBothProducerAndConsumer() {
        String variableName = "person";
        registry.registerProducer(variableName, VariableType.ENTITY, "NER");
        registry.registerConsumer(variableName, VariableType.ENTITY, "CONTAINS");
        
        // Verify registry state
        assertTrue(registry.isProduced(variableName));
        assertEquals(1, registry.getProducers(variableName).size());
        assertEquals(1, registry.getConsumers(variableName).size());
        
        // Check all variables
        assertEquals(Set.of("?person"), registry.getAllVariableNames());
    }

    @Test
    public void testMultipleProducers() {
        String variableName = "date";
        registry.registerProducer(variableName, VariableType.TEMPORAL, "DATE1");
        registry.registerProducer(variableName, VariableType.TEMPORAL, "DATE2");
        
        // Verify registry state
        assertEquals(2, registry.getProducers(variableName).size());
        
        // Test all producers collection
        Collection<ProducerVariable> allProducers = registry.getAllProducers();
        assertEquals(2, allProducers.size());
    }

    @Test
    public void testInferredType() {
        // Test with single type
        registry.registerProducer("person", VariableType.ENTITY, "NER");
        assertEquals(VariableType.ENTITY, registry.getInferredType("person"));
        
        // Test with same types
        registry.registerConsumer("person", VariableType.ENTITY, "CONTAINS");
        assertEquals(VariableType.ENTITY, registry.getInferredType("person"));
        
        // Test with conflicting types
        registry.registerConsumer("person", VariableType.TEXT_SPAN, "SNIPPET");
        assertEquals(VariableType.ANY, registry.getInferredType("person"));
        
        // Test with ANY type
        registry.registerProducer("unknown", VariableType.ANY, "CUSTOM");
        registry.registerConsumer("unknown", VariableType.ENTITY, "USE");
        assertEquals(VariableType.ENTITY, registry.getInferredType("unknown"));
    }

    @Test
    public void testValidation() {
        // Valid case: all consumed variables are produced
        registry.registerProducer("person", VariableType.ENTITY, "NER");
        registry.registerConsumer("person", VariableType.ENTITY, "CONTAINS");
        Set<String> errors = registry.validate();
        assertTrue(errors.isEmpty());
        
        // Invalid case: consumed variable not produced
        registry.clear();
        registry.registerConsumer("person", VariableType.ENTITY, "CONTAINS");
        errors = registry.validate();
        assertFalse(errors.isEmpty());
        assertTrue(errors.iterator().next().contains("?person is consumed but never produced"));
    }

    @Test
    public void testClear() {
        registry.registerProducer("person", VariableType.ENTITY, "NER");
        registry.registerConsumer("org", VariableType.ENTITY, "CONTAINS");
        
        // Clear and verify
        registry.clear();
        assertTrue(registry.getAllVariableNames().isEmpty());
        assertTrue(registry.getAllProducers().isEmpty());
        assertTrue(registry.getAllConsumers().isEmpty());
    }
} 
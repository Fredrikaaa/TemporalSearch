package com.example.query.binding;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the DefaultBindingContext class.
 */
public class DefaultBindingContextTest {

    private DefaultBindingContext context;

    @BeforeEach
    public void setUp() {
        context = new DefaultBindingContext();
    }

    @Test
    public void testBindSingleValue() {
        String variableName = "person";
        String value = "John Doe";
        
        context.bindValue(variableName, value);
        
        // Verify binding
        assertTrue(context.hasValue(variableName));
        assertEquals(Optional.of(value), context.getValue(variableName, String.class));
        assertEquals(List.of(value), context.getValues(variableName, String.class));
    }

    @Test
    public void testBindMultipleValues() {
        String variableName = "numbers";
        List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
        
        context.bindValues(variableName, values);
        
        // Verify binding
        assertTrue(context.hasValue(variableName));
        assertEquals(Optional.of(1), context.getValue(variableName, Integer.class));
        assertEquals(values, context.getValues(variableName, Integer.class));
    }

    @Test
    public void testVariableFormatting() {
        // Test with and without ? prefix
        context.bindValue("?person", "John");
        context.bindValue("location", "New York");
        
        // Both should be normalized to have the ? prefix
        assertTrue(context.hasValue("?person"));
        assertTrue(context.hasValue("?location"));
        assertTrue(context.hasValue("location"));
        
        // Check variable names
        Set<String> names = context.getVariableNames();
        assertEquals(2, names.size());
        assertTrue(names.contains("?person"));
        assertTrue(names.contains("?location"));
    }

    @Test
    public void testTypeFiltering() {
        String variableName = "mixed";
        context.bindValue(variableName, "text");
        context.bindValue(variableName, 42);
        context.bindValue(variableName, 3.14);
        
        // Filter by type
        assertEquals(List.of("text"), context.getValues(variableName, String.class));
        assertEquals(List.of(42), context.getValues(variableName, Integer.class));
        assertEquals(List.of(3.14), context.getValues(variableName, Double.class));
        
        // Get first value of each type
        assertEquals(Optional.of("text"), context.getValue(variableName, String.class));
        assertEquals(Optional.of(42), context.getValue(variableName, Integer.class));
        assertEquals(Optional.of(3.14), context.getValue(variableName, Double.class));
    }

    @Test
    public void testNullAndEmptyValues() {
        String variableName = "test";
        
        // Null values should be ignored
        context.bindValue(variableName, null);
        assertFalse(context.hasValue(variableName));
        
        // Empty collections should be ignored
        context.bindValues(variableName, List.of());
        assertFalse(context.hasValue(variableName));
    }

    @Test
    public void testCopy() {
        context.bindValue("person", "Alice");
        context.bindValue("age", 30);
        
        // Create a copy
        BindingContext copy = context.copy();
        
        // Verify copy has the same values
        assertEquals(context.getVariableNames(), copy.getVariableNames());
        assertEquals(Optional.of("Alice"), copy.getValue("person", String.class));
        assertEquals(Optional.of(30), copy.getValue("age", Integer.class));
        
        // Modify original should not affect copy
        context.bindValue("person", "Bob");
        assertEquals(Optional.of("Alice"), copy.getValue("person", String.class));
    }

    @Test
    public void testMerge() {
        // Set up two contexts
        context.bindValue("person", "Alice");
        context.bindValue("shared", "original");
        
        DefaultBindingContext other = new DefaultBindingContext();
        other.bindValue("age", 30);
        other.bindValue("shared", "updated");
        
        // Merge
        BindingContext merged = context.merge(other);
        
        // Verify merged values
        assertEquals(Optional.of("Alice"), merged.getValue("person", String.class));
        assertEquals(Optional.of(30), merged.getValue("age", Integer.class));
        
        // Shared variable should have both values, with 'updated' being the first (from 'other')
        List<String> sharedValues = merged.getValues("shared", String.class);
        assertEquals(2, sharedValues.size());
        assertTrue(sharedValues.contains("original"));
        assertTrue(sharedValues.contains("updated"));
    }

    @Test
    public void testEmptyStaticFactory() {
        BindingContext empty = BindingContext.empty();
        assertTrue(empty.getVariableNames().isEmpty());
    }

    @Test
    public void testMapConstructor() {
        Map<String, Object> initialBindings = new HashMap<>();
        initialBindings.put("person", "Alice");
        initialBindings.put("numbers", Arrays.asList(1, 2, 3));
        
        BindingContext fromMap = BindingContext.of(initialBindings);
        
        // Verify values
        assertEquals(Optional.of("Alice"), fromMap.getValue("person", String.class));
        assertEquals(Arrays.asList(1, 2, 3), fromMap.getValues("numbers", Integer.class));
        assertEquals(2, fromMap.getVariableNames().size());
    }
} 
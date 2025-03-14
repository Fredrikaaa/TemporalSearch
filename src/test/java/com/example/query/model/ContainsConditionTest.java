package com.example.query.model;

import org.junit.jupiter.api.Test;

import com.example.query.model.condition.Contains;

import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ContainsCondition Tests")
class ContainsConditionTest {

    @Test
    @DisplayName("Constructor should set value correctly")
    void constructorShouldSetValue() {
        String value = "test value";
        Contains condition = new Contains(value);
        assertEquals(value, condition.getValue());
    }

    @Test
    @DisplayName("getType should return CONTAINS")
    void getTypeShouldReturnContains() {
        Contains condition = new Contains("test");
        assertEquals("CONTAINS", condition.getType());
    }

    @Test
    @DisplayName("toString should include value")
    void toStringShouldIncludeValue() {
        String value = "test value";
        Contains condition = new Contains(value);
        String str = condition.toString();
        assertTrue(str.contains(value));
        assertTrue(str.contains("CONTAINS"));
    }

    @Test
    @DisplayName("Constructor should not accept null value")
    void constructorShouldNotAcceptNull() {
        String nullValue = null;
        assertThrows(NullPointerException.class, () -> new Contains(nullValue));
    }
} 
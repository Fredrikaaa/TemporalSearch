package com.example.query.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ContainsCondition Tests")
class ContainsConditionTest {

    @Test
    @DisplayName("Constructor should set value correctly")
    void constructorShouldSetValue() {
        String value = "test value";
        ContainsCondition condition = new ContainsCondition(value);
        assertEquals(value, condition.getValue());
    }

    @Test
    @DisplayName("getType should return CONTAINS")
    void getTypeShouldReturnContains() {
        ContainsCondition condition = new ContainsCondition("test");
        assertEquals("CONTAINS", condition.getType());
    }

    @Test
    @DisplayName("toString should include value")
    void toStringShouldIncludeValue() {
        String value = "test value";
        ContainsCondition condition = new ContainsCondition(value);
        String str = condition.toString();
        assertTrue(str.contains(value));
        assertTrue(str.contains("ContainsCondition"));
    }

    @Test
    @DisplayName("Constructor should not accept null value")
    void constructorShouldNotAcceptNull() {
        String nullValue = null;
        assertThrows(NullPointerException.class, () -> new ContainsCondition(nullValue));
    }
} 
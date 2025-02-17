package com.example.query.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NerCondition Tests")
class NerConditionTest {

    @Test
    @DisplayName("Constructor should set all fields correctly")
    void constructorShouldSetFields() {
        String type = "PERSON";
        String target = "Einstein";
        boolean isVariable = false;

        NerCondition condition = new NerCondition(type, target, isVariable);

        assertEquals(type, condition.getEntityType());
        assertEquals(target, condition.getTarget());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("getType should return NER")
    void getTypeShouldReturnNer() {
        NerCondition condition = new NerCondition("PERSON", "Einstein", false);
        assertEquals("NER", condition.getType());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("isVariable should return correct value")
    void isVariableShouldReturnCorrectValue(boolean isVariable) {
        NerCondition condition = new NerCondition("PERSON", "test", isVariable);
        assertEquals(isVariable, condition.isVariable());
    }

    @Test
    @DisplayName("toString should include all fields")
    void toStringShouldIncludeAllFields() {
        String type = "ORGANIZATION";
        String target = "Google";
        boolean isVariable = true;

        NerCondition condition = new NerCondition(type, target, isVariable);
        String str = condition.toString();

        assertTrue(str.contains(type));
        assertTrue(str.contains(target));
        assertTrue(str.contains(String.valueOf(isVariable)));
        assertTrue(str.contains("NerCondition"));
    }

    @Test
    @DisplayName("Constructor should not accept null type")
    void constructorShouldNotAcceptNullType() {
        assertThrows(NullPointerException.class, 
            () -> new NerCondition(null, "target", false));
    }

    @Test
    @DisplayName("Constructor should not accept null target")
    void constructorShouldNotAcceptNullTarget() {
        assertThrows(NullPointerException.class, 
            () -> new NerCondition("PERSON", null, false));
    }

    @Test
    @DisplayName("Constructor should accept common NER types")
    void constructorShouldAcceptCommonNerTypes() {
        String[] types = {"PERSON", "ORGANIZATION", "LOCATION", "DATE", "TIME"};
        
        for (String type : types) {
            NerCondition condition = new NerCondition(type, "test", false);
            assertEquals(type, condition.getEntityType());
        }
    }
} 
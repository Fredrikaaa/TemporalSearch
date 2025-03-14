package com.example.query.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.example.query.model.condition.Ner;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NerCondition Tests")
class NerConditionTest {

    @Test
    @DisplayName("Constructor should set fields correctly")
    void constructorShouldSetFields() {
        String type = "PERSON";
        String variableName = "scientist";

        Ner condition = new Ner(type, variableName);

        assertEquals(type, condition.entityType());
        assertEquals(variableName, condition.variableName());
        assertTrue(condition.isVariable());
    }

    @Test
    @DisplayName("getType should return NER")
    void getTypeShouldReturnNer() {
        Ner condition = Ner.of("PERSON");
        assertEquals("NER", condition.getType());
    }

    @Test
    @DisplayName("toString should include all fields")
    void toStringShouldIncludeAllFields() {
        String type = "ORGANIZATION";
        String variableName = "company";

        Ner condition = new Ner(type, variableName);
        String str = condition.toString();

        assertTrue(str.contains(type));
        assertTrue(str.contains(variableName));
        assertTrue(str.contains("NER"));
    }

    @Test
    @DisplayName("Constructor should not accept null type")
    void constructorShouldNotAcceptNullType() {
        assertThrows(NullPointerException.class, 
            () -> new Ner(null, "varName"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "PERSON", "ORGANIZATION", "LOCATION", "DATE", "TIME",
        "DURATION", "MONEY", "NUMBER", "ORDINAL", "PERCENT", "SET"
    })
    @DisplayName("Constructor should accept all supported NER types")
    void constructorShouldAcceptSupportedNerTypes(String type) {
        Ner condition = Ner.of(type);
        assertEquals(type, condition.entityType());
    }

    @Test
    @DisplayName("Entity type should be normalized to uppercase")
    void entityTypeShouldBeNormalizedToUppercase() {
        Ner condition = Ner.of("person");
        assertEquals("PERSON", condition.entityType());
    }

    @Test
    @DisplayName("of() should create condition without variable")
    void ofShouldCreateConditionWithoutVariable() {
        Ner condition = Ner.of("PERSON");
        assertEquals("PERSON", condition.entityType());
        assertNull(condition.variableName());
        assertFalse(condition.isVariable());
    }

    @Test
    @DisplayName("withVariable() should create condition with variable")
    void withVariableShouldCreateConditionWithVariable() {
        Ner condition = Ner.withVariable("PERSON", "?scientist");
        assertEquals("PERSON", condition.entityType());
        assertEquals("scientist", condition.variableName());
        assertTrue(condition.isVariable());
    }

    @Test
    @DisplayName("withVariable() should require ? prefix")
    void withVariableShouldRequireQuestionMarkPrefix() {
        assertThrows(IllegalArgumentException.class,
            () -> Ner.withVariable("PERSON", "scientist"));
    }

    @Test
    @DisplayName("withVariable() should normalize entity type")
    void withVariableShouldNormalizeEntityType() {
        Ner condition = Ner.withVariable("person", "?scientist");
        assertEquals("PERSON", condition.entityType());
    }

    @Test
    @DisplayName("toString should format without variable correctly")
    void toStringShouldFormatWithoutVariableCorrectly() {
        Ner condition = Ner.of("PERSON");
        assertEquals("NER(PERSON)", condition.toString());
    }

    @Test
    @DisplayName("toString should format with variable correctly")
    void toStringShouldFormatWithVariableCorrectly() {
        Ner condition = Ner.withVariable("PERSON", "?scientist");
        assertEquals("NER(PERSON, ?scientist)", condition.toString());
    }
} 
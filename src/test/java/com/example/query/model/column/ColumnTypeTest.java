package com.example.query.model.column;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

class ColumnTypeTest {

    @Test
    void testColumnTypeValues() {
        // Test all enum values are present
        assertNotNull(ColumnType.PERSON);
        assertNotNull(ColumnType.DATE);
        assertNotNull(ColumnType.LOCATION);
        assertNotNull(ColumnType.TERM);
        assertNotNull(ColumnType.RELATION);
        assertNotNull(ColumnType.CATEGORY);
        assertNotNull(ColumnType.SNIPPET);
        assertNotNull(ColumnType.COUNT);
    }

    @Test
    void testColumnSpecCreation() {
        // Test basic column spec creation
        ColumnSpec spec = new ColumnSpec(
            "personName",
            ColumnType.PERSON,
            "Person",
            Map.of("format", "full")
        );

        assertEquals("personName", spec.name());
        assertEquals(ColumnType.PERSON, spec.type());
        assertEquals("Person", spec.alias());
        assertEquals("full", spec.options().get("format"));
    }

    @Test
    void testColumnSpecWithNullOptionals() {
        // Test column spec creation with null optional values
        ColumnSpec spec = new ColumnSpec(
            "date",
            ColumnType.DATE,
            null,
            null
        );

        assertEquals("date", spec.name());
        assertEquals(ColumnType.DATE, spec.type());
        assertNull(spec.alias());
        assertNull(spec.options());
    }

    @Test
    void testColumnSpecValidation() {
        // Test that null required values throw exceptions
        assertThrows(NullPointerException.class, () -> 
            new ColumnSpec(null, ColumnType.PERSON, null, null)
        );
        
        assertThrows(NullPointerException.class, () -> 
            new ColumnSpec("test", null, null, null)
        );
    }
} 
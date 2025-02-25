package com.example.query.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

class TableConfigTest {

    @Test
    void testValidConfiguration() {
        // Create a custom TableConfig with specific values
        TableConfig config = new TableConfig(
            Map.of("format", "custom"), 
            true, 
            "CUSTOM_NULL", 
            60
        );
        
        // Test direct field access with the record
        assertEquals(Map.of("format", "custom"), config.formatOptions());
        assertTrue(config.showHeaders());
        assertEquals("CUSTOM_NULL", config.nullValueDisplay());
        assertEquals(60, config.maxColumnWidth());
    }

    @Test
    void testDefaultConfiguration() {
        TableConfig config = TableConfig.getDefault();
        
        // Test default values
        assertTrue(config.formatOptions().isEmpty());
        assertTrue(config.showHeaders());
        assertEquals("NULL", config.nullValueDisplay());
        assertEquals(50, config.maxColumnWidth());
    }

    @Test
    void testCompactConfiguration() {
        TableConfig config = TableConfig.compact();
        
        // Test compact config values
        assertFalse(config.showHeaders());
        assertEquals("-", config.nullValueDisplay());
        assertEquals(30, config.maxColumnWidth());
    }

    @Test
    void testWideConfiguration() {
        TableConfig config = TableConfig.wide();
        
        // Test wide config values
        assertTrue(config.showHeaders());
        assertEquals("NULL", config.nullValueDisplay());
        assertEquals(100, config.maxColumnWidth());
    }

    @Test
    void testWithMethods() {
        TableConfig config = TableConfig.getDefault()
            .withShowHeaders(false)
            .withNullValueDisplay("N/A")
            .withMaxColumnWidth(80)
            .withFormatOption("key", "value");
            
        assertFalse(config.showHeaders());
        assertEquals("N/A", config.nullValueDisplay());
        assertEquals(80, config.maxColumnWidth());
        assertEquals("value", config.formatOptions().get("key"));
    }

    @Test
    void testInvalidMaxColumnWidthThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            new TableConfig(Map.of(), true, "NULL", 0));
    }
} 
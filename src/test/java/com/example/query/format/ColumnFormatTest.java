package com.example.query.format;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ColumnFormatTest {

    @Test
    void testValidColumnFormat() {
        ColumnFormat format = new ColumnFormat(
            "test",
            "Test Column",
            20,
            ColumnFormat.TextAlign.LEFT,
            String::toUpperCase
        );

        assertEquals("test", format.name());
        assertEquals("Test Column", format.alias());
        assertEquals(20, format.width());
        assertEquals(ColumnFormat.TextAlign.LEFT, format.align());
        assertEquals("TEST", format.formatter().apply("test"));
    }

    @Test
    void testDefaultColumnFormat() {
        ColumnFormat format = ColumnFormat.createDefault("test", 20);

        assertEquals("test", format.name());
        assertEquals("test", format.alias()); // Uses name as alias
        assertEquals(20, format.width());
        assertEquals(ColumnFormat.TextAlign.LEFT, format.align());
        assertEquals("test", format.formatter().apply("test")); // Identity formatter
    }

    @Test
    void testNullNameThrowsException() {
        assertThrows(NullPointerException.class, () ->
            new ColumnFormat(
                null,
                "Test Column",
                20,
                ColumnFormat.TextAlign.LEFT,
                String::toUpperCase
            ));
    }

    @Test
    void testNullAlignmentThrowsException() {
        assertThrows(NullPointerException.class, () ->
            new ColumnFormat(
                "test",
                "Test Column",
                20,
                null,
                String::toUpperCase
            ));
    }

    @Test
    void testNullFormatterThrowsException() {
        assertThrows(NullPointerException.class, () ->
            new ColumnFormat(
                "test",
                "Test Column",
                20,
                ColumnFormat.TextAlign.LEFT,
                null
            ));
    }

    @Test
    void testInvalidWidthThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
            new ColumnFormat(
                "test",
                "Test Column",
                0,
                ColumnFormat.TextAlign.LEFT,
                String::toUpperCase
            ));
    }

    @Test
    void testNullAliasUsesName() {
        ColumnFormat format = new ColumnFormat(
            "test",
            null,
            20,
            ColumnFormat.TextAlign.LEFT,
            String::toUpperCase
        );

        assertEquals("test", format.alias());
    }
} 
package com.example.query.snippet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class HighlighterTest {

    @Test
    public void testHighlightWithBoldStyle() {
        Highlighter highlighter = new Highlighter("**");
        String result = highlighter.highlight("This is a test", 5, 9);
        assertEquals("This **is a** test", result);
    }

    @Test
    public void testHighlightWithCustomStyle() {
        Highlighter highlighter = new Highlighter("__");
        String result = highlighter.highlight("This is a test", 5, 9);
        assertEquals("This __is a__ test", result);
    }

    @Test
    public void testHighlightAtStart() {
        Highlighter highlighter = new Highlighter("**");
        String result = highlighter.highlight("This is a test", 0, 4);
        assertEquals("**This** is a test", result);
    }

    @Test
    public void testHighlightAtEnd() {
        Highlighter highlighter = new Highlighter("**");
        String result = highlighter.highlight("This is a test", 10, 14);
        assertEquals("This is a **test**", result);
    }

    @Test
    public void testHighlightEntireText() {
        Highlighter highlighter = new Highlighter("**");
        String result = highlighter.highlight("This is a test", 0, 14);
        assertEquals("**This is a test**", result);
    }

    @Test
    public void testInvalidStartPosition() {
        Highlighter highlighter = new Highlighter("**");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            highlighter.highlight("This is a test", -1, 5);
        });
        assertTrue(exception.getMessage().contains("Invalid match positions"));
    }

    @Test
    public void testInvalidEndPosition() {
        Highlighter highlighter = new Highlighter("**");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            highlighter.highlight("This is a test", 5, 20);
        });
        assertTrue(exception.getMessage().contains("Invalid match positions"));
    }

    @Test
    public void testStartGreaterThanEnd() {
        Highlighter highlighter = new Highlighter("**");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            highlighter.highlight("This is a test", 10, 5);
        });
        assertTrue(exception.getMessage().contains("Invalid match positions"));
    }
} 
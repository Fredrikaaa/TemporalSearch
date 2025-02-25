package com.example.query.snippet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class SnippetConfigTest {

    @Test
    public void testValidConstruction() {
        SnippetConfig config = new SnippetConfig(2, "**", true);
        assertEquals(2, config.windowSize());
        assertEquals("**", config.highlightStyle());
        assertTrue(config.showSentenceBoundaries());
    }

    @Test
    public void testDefaultConfig() {
        SnippetConfig config = SnippetConfig.DEFAULT;
        assertEquals(1, config.windowSize());
        assertEquals("**", config.highlightStyle());
        assertFalse(config.showSentenceBoundaries());
    }

    @Test
    public void testNegativeWindowSize() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetConfig(-1, "**", false);
        });
        assertTrue(exception.getMessage().contains("windowSize must be between 0 and 5"));
    }

    @Test
    public void testTooLargeWindowSize() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetConfig(6, "**", false);
        });
        assertTrue(exception.getMessage().contains("windowSize must be between 0 and 5"));
    }

    @Test
    public void testNullHighlightStyle() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetConfig(1, null, false);
        });
        assertTrue(exception.getMessage().contains("highlightStyle must not be null or empty"));
    }

    @Test
    public void testEmptyHighlightStyle() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetConfig(1, "", false);
        });
        assertTrue(exception.getMessage().contains("highlightStyle must not be null or empty"));
    }
} 
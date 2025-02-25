package com.example.query.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.example.query.snippet.SnippetConfig;

public class SnippetNodeTest {

    @Test
    public void testConstructorWithVariableOnly() {
        SnippetNode node = new SnippetNode("var");
        assertEquals("var", node.variable());
        assertEquals(SnippetNode.DEFAULT_WINDOW_SIZE, node.windowSize());
        assertEquals(SnippetNode.DEFAULT_HIGHLIGHT_STYLE, node.highlightStyle());
        assertEquals(SnippetNode.DEFAULT_SHOW_SENTENCE_BOUNDARIES, node.showSentenceBoundaries());
    }

    @Test
    public void testConstructorWithVariableAndWindowSize() {
        SnippetNode node = new SnippetNode("var", 2);
        assertEquals("var", node.variable());
        assertEquals(2, node.windowSize());
        assertEquals(SnippetNode.DEFAULT_HIGHLIGHT_STYLE, node.highlightStyle());
        assertEquals(SnippetNode.DEFAULT_SHOW_SENTENCE_BOUNDARIES, node.showSentenceBoundaries());
    }

    @Test
    public void testConstructorWithAllParameters() {
        SnippetNode node = new SnippetNode("var", 3, "__", true);
        assertEquals("var", node.variable());
        assertEquals(3, node.windowSize());
        assertEquals("__", node.highlightStyle());
        assertTrue(node.showSentenceBoundaries());
    }

    @Test
    public void testNullVariable() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetNode(null, 1, "**", false);
        });
        assertTrue(exception.getMessage().contains("variable must not be null or empty"));
    }

    @Test
    public void testEmptyVariable() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetNode("", 1, "**", false);
        });
        assertTrue(exception.getMessage().contains("variable must not be null or empty"));
    }

    @Test
    public void testNegativeWindowSize() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetNode("var", -1, "**", false);
        });
        assertTrue(exception.getMessage().contains("windowSize must be between 0 and 5"));
    }

    @Test
    public void testTooLargeWindowSize() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetNode("var", 6, "**", false);
        });
        assertTrue(exception.getMessage().contains("windowSize must be between 0 and 5"));
    }

    @Test
    public void testNullHighlightStyle() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetNode("var", 1, null, false);
        });
        assertTrue(exception.getMessage().contains("highlightStyle must not be null or empty"));
    }

    @Test
    public void testEmptyHighlightStyle() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetNode("var", 1, "", false);
        });
        assertTrue(exception.getMessage().contains("highlightStyle must not be null or empty"));
    }

    @Test
    public void testToSnippetConfig() {
        SnippetNode node = new SnippetNode("var", 2, "__", true);
        SnippetConfig config = node.toSnippetConfig();
        
        assertEquals(2, config.windowSize());
        assertEquals("__", config.highlightStyle());
        assertTrue(config.showSentenceBoundaries());
    }

    @Test
    public void testToStringWithDefaultValues() {
        SnippetNode node = new SnippetNode("var");
        assertEquals("SNIPPET(var)", node.toString());
    }

    @Test
    public void testToStringWithCustomWindowSize() {
        SnippetNode node = new SnippetNode("var", 2);
        assertEquals("SNIPPET(var, window=2)", node.toString());
    }

    @Test
    public void testToStringWithAllCustomValues() {
        SnippetNode node = new SnippetNode("var", 3, "__", true);
        assertEquals("SNIPPET(var, window=3, style=__, boundaries=true)", node.toString());
    }
} 
package com.example.query.snippet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ContextAnchorTest {

    @Test
    public void testValidConstruction() {
        ContextAnchor anchor = new ContextAnchor(1, 2, 3, "variable");
        assertEquals(1, anchor.documentId());
        assertEquals(2, anchor.sentenceId());
        assertEquals(3, anchor.tokenPosition());
        assertEquals("variable", anchor.variableName());
    }

    @Test
    public void testNegativeDocumentId() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new ContextAnchor(-1, 2, 3, "variable");
        });
        assertTrue(exception.getMessage().contains("documentId must be non-negative"));
    }

    @Test
    public void testNegativeSentenceId() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new ContextAnchor(1, -2, 3, "variable");
        });
        assertTrue(exception.getMessage().contains("sentenceId must be non-negative"));
    }

    @Test
    public void testEquality() {
        ContextAnchor anchor1 = new ContextAnchor(1, 2, 3, "variable");
        ContextAnchor anchor2 = new ContextAnchor(1, 2, 3, "variable");
        ContextAnchor anchor3 = new ContextAnchor(1, 2, 4, "variable");
        
        assertEquals(anchor1, anchor2);
        assertNotEquals(anchor1, anchor3);
    }
    
    @Test
    public void testHashCode() {
        ContextAnchor anchor1 = new ContextAnchor(1, 2, 3, "variable");
        ContextAnchor anchor2 = new ContextAnchor(1, 2, 3, "variable");
        
        assertEquals(anchor1.hashCode(), anchor2.hashCode());
    }
} 
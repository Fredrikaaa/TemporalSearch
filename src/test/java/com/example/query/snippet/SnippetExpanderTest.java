package com.example.query.snippet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SnippetExpanderTest {

    private Connection connection;
    private SnippetExpander expander;

    @BeforeEach
    public void setUp() throws SQLException {
        connection = MockDatabaseSetup.setupMockDatabase();
        expander = new SnippetExpander(connection);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void testExpandWithWindowSizeZero() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 1, 63, "match");
        List<SnippetExpander.SentenceContext> sentences = expander.expand(anchor, 0);
        
        assertEquals(1, sentences.size());
        assertEquals(1, sentences.get(0).sentenceId());
        assertTrue(sentences.get(0).isMatchSentence());
        assertTrue(sentences.get(0).text().contains("second sentence with a match"));
    }

    @Test
    public void testExpandWithWindowSizeOne() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 1, 63, "match");
        List<SnippetExpander.SentenceContext> sentences = expander.expand(anchor, 1);
        
        assertEquals(3, sentences.size());
        assertEquals(0, sentences.get(0).sentenceId());
        assertEquals(1, sentences.get(1).sentenceId());
        assertEquals(2, sentences.get(2).sentenceId());
        
        assertFalse(sentences.get(0).isMatchSentence());
        assertTrue(sentences.get(1).isMatchSentence());
        assertFalse(sentences.get(2).isMatchSentence());
        
        assertTrue(sentences.get(0).text().contains("first sentence"));
        assertTrue(sentences.get(1).text().contains("second sentence with a match"));
        assertTrue(sentences.get(2).text().contains("third sentence"));
    }

    @Test
    public void testExpandWithFirstSentence() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 0, 12, "first");
        List<SnippetExpander.SentenceContext> sentences = expander.expand(anchor, 1);
        
        assertEquals(2, sentences.size());
        assertEquals(0, sentences.get(0).sentenceId());
        assertEquals(1, sentences.get(1).sentenceId());
        
        assertTrue(sentences.get(0).isMatchSentence());
        assertFalse(sentences.get(1).isMatchSentence());
    }

    @Test
    public void testExpandWithLastSentence() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 2, 86, "third");
        List<SnippetExpander.SentenceContext> sentences = expander.expand(anchor, 1);
        
        assertEquals(2, sentences.size());
        assertEquals(1, sentences.get(0).sentenceId());
        assertEquals(2, sentences.get(1).sentenceId());
        
        assertFalse(sentences.get(0).isMatchSentence());
        assertTrue(sentences.get(1).isMatchSentence());
    }

    @Test
    public void testNullConnection() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            new SnippetExpander(null);
        });
        assertTrue(exception.getMessage().contains("connection must not be null"));
    }
} 
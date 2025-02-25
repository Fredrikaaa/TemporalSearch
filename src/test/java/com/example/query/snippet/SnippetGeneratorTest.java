package com.example.query.snippet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SnippetGeneratorTest {

    private Connection connection;
    private SnippetGenerator generator;

    @BeforeEach
    public void setUp() throws SQLException {
        connection = MockDatabaseSetup.setupMockDatabase();
        generator = new SnippetGenerator(connection);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    public void testGenerateSnippet() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 1, 63, "matchVar");
        TableSnippet snippet = generator.generateSnippet(anchor);
        
        assertNotNull(snippet);
        assertFalse(snippet.text().isEmpty());
        assertEquals(1, snippet.highlights().size());
        assertTrue(snippet.matchValues().containsKey("matchVar"));
        
        // Check that the highlight is correctly positioned
        TableSnippet.SimpleSpan highlight = snippet.highlights().get(0);
        assertEquals("matchVar", highlight.type());
        assertTrue(snippet.text().substring(highlight.start(), highlight.end()).contains("match"));
    }

    @Test
    public void testGenerateSnippetWithCustomConfig() throws SQLException {
        SnippetConfig config = new SnippetConfig(2, "**", true);
        SnippetGenerator customGenerator = new SnippetGenerator(connection, config);
        
        ContextAnchor anchor = new ContextAnchor(1, 1, 63, "matchVar");
        TableSnippet snippet = customGenerator.generateSnippet(anchor);
        
        assertNotNull(snippet);
        assertTrue(snippet.text().contains("|")); // Should contain sentence boundaries
    }

    @Test
    public void testFormatSnippet() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 1, 63, "matchVar");
        TableSnippet snippet = generator.generateSnippet(anchor);
        
        String formatted = generator.formatSnippet(snippet);
        
        assertNotNull(formatted);
        assertFalse(formatted.isEmpty());
        assertTrue(formatted.contains("**")); // Should contain bold formatting
    }

    @Test
    public void testGenerateSnippetWithFirstSentence() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 0, 12, "firstVar");
        TableSnippet snippet = generator.generateSnippet(anchor);
        
        assertNotNull(snippet);
        assertEquals(1, snippet.highlights().size());
        assertTrue(snippet.matchValues().containsKey("firstVar"));
    }

    @Test
    public void testGenerateSnippetWithLastSentence() throws SQLException {
        ContextAnchor anchor = new ContextAnchor(1, 2, 86, "thirdVar");
        TableSnippet snippet = generator.generateSnippet(anchor);
        
        assertNotNull(snippet);
        assertEquals(1, snippet.highlights().size());
        assertTrue(snippet.matchValues().containsKey("thirdVar"));
    }
} 
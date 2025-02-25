package com.example.snippet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.example.query.snippet.SnippetGenerator;
import com.example.query.snippet.TableSnippet;
import com.example.query.snippet.SnippetConfig;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class SnippetGeneratorTest {
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    private SnippetGenerator generator;
    
    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        
        generator = new SnippetGenerator(connection);
    }
    
    @Test
    void testGenerateSnippet() throws SQLException {
        // Setup mock data
        String sampleText = "Barack Obama was president from 2009 to 2017.";
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("text")).thenReturn(sampleText);
        
        // Generate snippet for "Obama"
        TableSnippet snippet = generator.generateSnippet(
            1L,      // documentId
            1,       // sentenceId
            7, 12,   // match position (Obama)
            "PERSON", // match type
            "Obama"   // match value
        );
        
        // Verify snippet text includes context
        assertTrue(snippet.text().contains("Barack Obama was president"));
        
        // Verify highlight position
        assertEquals(1, snippet.highlights().size());
        var highlight = snippet.highlights().get(0);
        assertEquals("PERSON", highlight.type());
        assertEquals("Obama", highlight.value());
        
        // Verify formatted output
        String formatted = generator.formatSnippet(snippet);
        assertTrue(formatted.contains("*PERSON:Obama*"));
    }
    
    @Test
    void testSnippetTruncation() throws SQLException {
        // Setup mock data with long text
        String longText = "This is a very long sentence that contains the name Barack Obama " +
                         "somewhere in the middle and continues for quite a while with more " +
                         "text that should be truncated in the final snippet output.";
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("text")).thenReturn(longText);
        
        // Use custom config with small context
        SnippetConfig config = new SnippetConfig(20, 50);
        generator = new SnippetGenerator(connection, config);
        
        // Generate snippet for "Obama"
        TableSnippet snippet = generator.generateSnippet(
            1L,      // documentId
            1,       // sentenceId
            45, 50,  // match position (Obama)
            "PERSON", // match type
            "Obama"   // match value
        );
        
        // Verify snippet length is within limits
        assertTrue(snippet.text().length() <= config.maxLength() + 6); // +6 for ellipsis
        assertTrue(snippet.text().startsWith("..."));
        assertTrue(snippet.text().endsWith("..."));
        
        // Verify highlight is present
        String formatted = generator.formatSnippet(snippet);
        assertTrue(formatted.contains("*PERSON:Obama*"));
    }
    
    @Test
    void testInvalidSentence() throws SQLException {
        // Setup mock to return no results
        when(resultSet.next()).thenReturn(false);
        
        // Verify exception is thrown
        assertThrows(IllegalStateException.class, () ->
            generator.generateSnippet(1L, 1, 0, 5, "PERSON", "Test")
        );
    }
    
    @Test
    void testMultipleHighlights() {
        String text = "Barack Obama was president in 2009.";
        TableSnippet snippet = new TableSnippet(
            text,
            List.of(
                new TableSnippet.SimpleSpan("PERSON", "Obama", 7, 12),
                new TableSnippet.SimpleSpan("DATE", "2009", 29, 33)
            ),
            Map.of(
                "PERSON", "Obama",
                "DATE", "2009"
            )
        );
        
        String formatted = generator.formatSnippet(snippet);
        assertTrue(formatted.contains("*PERSON:Obama*"));
        assertTrue(formatted.contains("*DATE:2009*"));
    }
} 
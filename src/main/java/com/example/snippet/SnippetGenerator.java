package com.example.snippet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Generates formatted text snippets for table display with highlights and extracted values
 */
public class SnippetGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SnippetGenerator.class);
    
    private static final String HIGHLIGHT_PREFIX = "*";
    private static final String HIGHLIGHT_SUFFIX = "*";
    private static final String TYPE_SEPARATOR = ":";
    
    private final Connection connection;
    private final SnippetConfig config;
    
    /**
     * Creates a new SnippetGenerator with default configuration
     * @param connection Database connection for fetching text and annotations
     */
    public SnippetGenerator(Connection connection) {
        this(connection, SnippetConfig.DEFAULT);
    }
    
    /**
     * Creates a new SnippetGenerator with custom configuration
     * @param connection Database connection for fetching text and annotations
     * @param config Custom snippet configuration
     */
    public SnippetGenerator(Connection connection, SnippetConfig config) {
        this.connection = connection;
        this.config = config;
    }
    
    /**
     * Generates a snippet for a match in a document
     * @param documentId ID of the document containing the match
     * @param sentenceId ID of the sentence containing the match
     * @param matchStart Start position of the match in the sentence
     * @param matchEnd End position of the match in the sentence
     * @param matchType Type of the match (e.g. PERSON, DATE)
     * @param matchValue Extracted value from the match
     * @return Generated snippet with highlights and values
     */
    public TableSnippet generateSnippet(
            long documentId,
            int sentenceId,
            int matchStart,
            int matchEnd,
            String matchType,
            String matchValue
    ) throws SQLException {
        // Fetch the sentence text and position
        String sentenceText = fetchSentenceText(documentId, sentenceId);
        if (sentenceText == null) {
            throw new IllegalStateException("Sentence not found: " + sentenceId);
        }
        
        // Calculate context window
        int contextStart = Math.max(0, matchStart - config.contextChars());
        int contextEnd = Math.min(sentenceText.length(), matchEnd + config.contextChars());
        
        // Truncate if exceeds max length
        if (contextEnd - contextStart > config.maxLength()) {
            int excess = (contextEnd - contextStart) - config.maxLength();
            contextStart += excess / 2;
            contextEnd -= excess / 2;
            if (excess % 2 == 1) contextEnd--;
        }
        
        // Extract the snippet text
        String snippetText = sentenceText.substring(contextStart, contextEnd);
        
        // Add ellipsis if truncated
        if (contextStart > 0) {
            snippetText = "..." + snippetText;
        }
        if (contextEnd < sentenceText.length()) {
            snippetText = snippetText + "...";
        }
        
        // Create highlight span adjusted for context window and ellipsis
        int adjustedStart = matchStart - contextStart + (contextStart > 0 ? 3 : 0);
        int adjustedEnd = matchEnd - contextStart + (contextStart > 0 ? 3 : 0);
        var highlight = new TableSnippet.SimpleSpan(
            matchType,
            matchValue,
            adjustedStart,
            adjustedEnd
        );
        
        // Create match values map
        Map<String, String> matchValues = new HashMap<>();
        matchValues.put(matchType, matchValue);
        
        return new TableSnippet(
            snippetText,
            List.of(highlight),
            matchValues
        );
    }
    
    /**
     * Formats a snippet with highlights for display
     * @param snippet The snippet to format
     * @return Formatted text with highlights
     */
    public String formatSnippet(TableSnippet snippet) {
        StringBuilder result = new StringBuilder(snippet.text());
        
        // Sort highlights in reverse order to avoid position shifts
        List<TableSnippet.SimpleSpan> sortedHighlights = new ArrayList<>(snippet.highlights());
        sortedHighlights.sort((a, b) -> Integer.compare(b.start(), a.start()));
        
        // Apply highlights
        for (var highlight : sortedHighlights) {
            String highlightText = HIGHLIGHT_PREFIX +
                                 highlight.type() +
                                 TYPE_SEPARATOR +
                                 highlight.value() +
                                 HIGHLIGHT_SUFFIX;
            result.replace(
                highlight.start(),
                highlight.end(),
                highlightText
            );
        }
        
        return result.toString();
    }
    
    private String fetchSentenceText(long documentId, int sentenceId) throws SQLException {
        String sql = """
            SELECT text, begin_char, end_char
            FROM sentences
            WHERE document_id = ? AND sentence_id = ?
            """;
            
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, documentId);
            stmt.setInt(2, sentenceId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("text");
                }
                return null;
            }
        }
    }
} 
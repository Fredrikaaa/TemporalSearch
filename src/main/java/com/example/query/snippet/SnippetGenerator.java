package com.example.query.snippet;

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
    
    private final Connection connection;
    private final SnippetConfig config;
    private final SnippetExpander expander;
    private final Highlighter highlighter;
    
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
        this.expander = new SnippetExpander(connection);
        this.highlighter = new Highlighter(config.highlightStyle());
        logger.debug("Created SnippetGenerator with config: windowSize={}, highlightStyle={}, showSentenceBoundaries={}",
            config.windowSize(), config.highlightStyle(), config.showSentenceBoundaries());
    }
    
    /**
     * Generates a snippet for a match in a document
     * @param anchor The anchor point to generate a snippet for
     * @return Generated snippet with highlights and values
     * @throws SQLException if a database access error occurs
     */
    public TableSnippet generateSnippet(ContextAnchor anchor) throws SQLException {
        logger.debug("Generating snippet for anchor: documentId={}, sentenceId={}, variableName={}",
            anchor.documentId(), anchor.sentenceId(), anchor.variableName());
            
        // Expand context around anchor
        List<SnippetExpander.SentenceContext> sentences = expander.expand(anchor, config.windowSize());
        
        if (sentences.isEmpty()) {
            logger.warn("No sentences found for anchor: {}", anchor);
            throw new IllegalStateException("No sentences found for anchor: " + anchor);
        }
        
        // Find the match sentence
        SnippetExpander.SentenceContext matchSentence = sentences.stream()
            .filter(SnippetExpander.SentenceContext::isMatchSentence)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Match sentence not found in context window"));
            
        // Get token information for highlighting
        int matchStart = -1;
        int matchEnd = -1;
        String matchValue = null;
        
        String sql = """
            SELECT begin_char, end_char, token, ner
            FROM annotations
            WHERE document_id = ? AND sentence_id = ? AND begin_char <= ? AND end_char >= ?
            """;
            
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, anchor.documentId());
            stmt.setInt(2, anchor.sentenceId());
            stmt.setInt(3, anchor.tokenPosition());
            stmt.setInt(4, anchor.tokenPosition());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    matchStart = rs.getInt("begin_char") - matchSentence.beginChar();
                    matchEnd = rs.getInt("end_char") - matchSentence.beginChar();
                    matchValue = rs.getString("token");
                }
            }
        }
        
        if (matchStart == -1 || matchEnd == -1) {
            logger.warn("Could not find token at position {} in sentence {}", 
                anchor.tokenPosition(), anchor.sentenceId());
            // Fall back to using the whole sentence
            matchStart = 0;
            matchEnd = matchSentence.text().length();
            matchValue = matchSentence.text();
        }
        
        // Build the snippet text
        StringBuilder snippetText = new StringBuilder();
        List<TableSnippet.SimpleSpan> highlights = new ArrayList<>();
        int currentPosition = 0;
        
        for (var sentence : sentences) {
            if (config.showSentenceBoundaries() && snippetText.length() > 0) {
                snippetText.append(" | ");
                currentPosition += 3;
            } else if (snippetText.length() > 0) {
                snippetText.append(" ");
                currentPosition += 1;
            }
            
            // Add the sentence text
            String sentenceText = sentence.text();
            snippetText.append(sentenceText);
            
            // If this is the match sentence, add a highlight
            if (sentence.isMatchSentence()) {
                int highlightStart = currentPosition + matchStart;
                int highlightEnd = currentPosition + matchEnd;
                
                highlights.add(new TableSnippet.SimpleSpan(
                    anchor.variableName(),
                    matchValue,
                    highlightStart,
                    highlightEnd
                ));
            }
            
            currentPosition += sentenceText.length();
        }
        
        // Create match values map
        Map<String, String> matchValues = new HashMap<>();
        matchValues.put(anchor.variableName(), matchValue);
        
        return new TableSnippet(
            snippetText.toString(),
            highlights,
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
            String highlightedText = snippet.text().substring(highlight.start(), highlight.end());
            String formattedText = highlighter.highlight(highlightedText, 0, highlightedText.length());
            
            result.replace(
                highlight.start(),
                highlight.end(),
                formattedText
            );
        }
        
        return result.toString();
    }
} 
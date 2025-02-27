package com.example.query.snippet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * A snippet generator that extracts text snippets from documents.
 * Uses the document text and annotations table to generate snippets.
 * Optimized for performance with efficient queries.
 */
public class SnippetGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SnippetGenerator.class);
    
    private final Connection connection;
    private final int windowSize;
    private final String highlightStyle;
    private final boolean showSentenceBoundaries;
    
    // Cache to avoid redundant document text queries
    private final Map<Long, String> documentTextCache = new HashMap<>();
    
    /**
     * Creates a new SnippetGenerator with default configuration.
     * 
     * @param connection Database connection for fetching text and annotations
     */
    public SnippetGenerator(Connection connection) {
        this(connection, 1, "**", false);
    }
    
    /**
     * Creates a new SnippetGenerator with custom configuration.
     * 
     * @param connection Database connection for fetching text and annotations
     * @param windowSize Number of context tokens to show on each side of the match
     * @param highlightStyle Style of highlighting (e.g., "**" for bold)
     * @param showSentenceBoundaries Whether to show sentence boundaries in snippets
     */
    public SnippetGenerator(Connection connection, int windowSize, 
                                      String highlightStyle, boolean showSentenceBoundaries) {
        this.connection = connection;
        this.windowSize = windowSize;
        this.highlightStyle = highlightStyle;
        this.showSentenceBoundaries = showSentenceBoundaries;
        
        logger.debug("Created SnippetGenerator with config: windowSize={}, highlightStyle={}, showSentenceBoundaries={}",
            windowSize, highlightStyle, showSentenceBoundaries);
    }
    
    /**
     * Efficiently generates a formatted snippet for a match in a document.
     * 
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param tokenPosition The position of the token in the sentence
     * @param variableName The name of the variable that matched
     * @return Formatted snippet text with highlights
     * @throws SQLException if a database error occurs
     */
    public String generateSnippet(long documentId, int sentenceId, int tokenPosition, String variableName) 
            throws SQLException {
        logger.debug("Generating snippet for documentId={}, sentenceId={}, tokenPosition={}, variableName={}",
            documentId, sentenceId, tokenPosition, variableName);
        
        // 1. Get document text (using cache for efficiency)
        String documentText = getDocumentText(documentId);
        if (documentText == null || documentText.isEmpty()) {
            logger.warn("Document text not found: documentId={}", documentId);
            return "[Document text not found]";
        }
        
        // Skip further processing if tokenPosition is out of bounds
        if (tokenPosition < 0 || tokenPosition >= documentText.length()) {
            logger.warn("Token position {} is out of bounds for document {} (length: {})",
                tokenPosition, documentId, documentText.length());
            return generatePositionBasedSnippet(documentText, 
                Math.min(documentText.length() - 1, Math.max(0, tokenPosition)),
                variableName);
        }
        
        // For document-level matches, find the appropriate sentence efficiently
        if (sentenceId < 0) {
            sentenceId = findSentenceForPosition(documentId, tokenPosition);
            logger.debug("For document-level match, found sentence {} for position {}", sentenceId, tokenPosition);
            
            if (sentenceId < 0) {
                // Use a direct position-based approach if we can't find a sentence
                return generatePositionBasedSnippet(documentText, tokenPosition, variableName);
            }
        }
        
        // Find the token efficiently with an optimized query
        MatchInfo matchInfo = findNearestToken(documentId, sentenceId, tokenPosition);
        if (matchInfo == null) {
            // Fall back to position-based snippet if we can't find the token
            return generatePositionBasedSnippet(documentText, tokenPosition, variableName);
        }
        
        // Generate snippet with the found token
        return extractContextSnippet(
            documentText, 
            matchInfo.beginPos, 
            matchInfo.endPos, 
            matchInfo.token,
            variableName
        );
    }
    
    /**
     * Generates a snippet based directly on character position in document text.
     * Used as a fallback when we can't find proper token information.
     *
     * @param documentText The full document text
     * @param position The character position
     * @param variableName The variable name
     * @return A snippet showing context around the position
     */
    private String generatePositionBasedSnippet(String documentText, int position, String variableName) {
        // Find word boundaries around the position
        int start = position;
        int end = position;
        
        // Find start of word (non-whitespace character)
        while (start > 0 && !Character.isWhitespace(documentText.charAt(start - 1))) {
            start--;
        }
        
        // Find end of word
        while (end < documentText.length() && !Character.isWhitespace(documentText.charAt(end))) {
            end++;
        }
        
        // Extract context characters
        int contextChars = 50;
        int snippetStart = Math.max(0, start - contextChars);
        int snippetEnd = Math.min(documentText.length(), end + contextChars);
        
        // Create snippet
        String snippetText = documentText.substring(snippetStart, snippetEnd);
        
        // Adjust word position in snippet
        int highlightStart = start - snippetStart;
        int highlightEnd = end - snippetStart;
        
        // Apply highlighting
        if (highlightStart >= 0 && highlightEnd <= snippetText.length() && highlightStart < highlightEnd) {
            StringBuilder result = new StringBuilder(snippetText);
            result.insert(highlightEnd, highlightStyle);
            result.insert(highlightStart, highlightStyle);
            return result.toString();
        }
        
        return snippetText;
    }
    
    /**
     * Efficiently finds which sentence contains a given character position using a single query.
     *
     * @param documentId The document ID
     * @param charPosition The character position
     * @return The sentence ID, or -1 if not found
     * @throws SQLException if a database error occurs
     */
    private int findSentenceForPosition(long documentId, int charPosition) throws SQLException {
        if (charPosition < 0) {
            return -1;
        }
        
        // Single optimized query to find the sentence containing or nearest to the position
        String sql = """
            SELECT sentence_id 
            FROM annotations
            WHERE document_id = ? 
            ORDER BY ABS(begin_char - ?)
            LIMIT 1
            """;
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, documentId);
            stmt.setInt(2, charPosition);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("sentence_id");
                }
            }
        }
        
        return -1;
    }
    
    /**
     * Efficiently finds the nearest token to a position in a single query.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param position The character position
     * @return Information about the token, or null if not found
     * @throws SQLException if a database error occurs
     */
    private MatchInfo findNearestToken(long documentId, int sentenceId, int position) throws SQLException {
        if (position < 0) {
            return null;
        }
        
        // Optimized query to find the nearest token in one step
        String sql = """
            SELECT begin_char, end_char, token, sentence_id
            FROM annotations
            WHERE document_id = ?
            AND (sentence_id = ? OR ? = -1)
            ORDER BY ABS(begin_char - ?)
            LIMIT 1
            """;
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, documentId);
            stmt.setInt(2, sentenceId);
            stmt.setInt(3, sentenceId); // If sentenceId is -1, we'll match any sentence
            stmt.setInt(4, position);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int beginChar = rs.getInt("begin_char");
                    int endChar = rs.getInt("end_char");
                    String token = rs.getString("token");
                    
                    logger.debug("Found token '{}' at position [{},{}]", token, beginChar, endChar);
                    
                    return new MatchInfo(
                        token,
                        beginChar,
                        endChar,
                        token
                    );
                }
            }
        }
        
        return null;
    }
    
    /**
     * Gets the full text of a document, using cache for efficiency.
     * 
     * @param documentId The document ID
     * @return The document text, or null if not found
     * @throws SQLException if a database error occurs
     */
    private String getDocumentText(long documentId) throws SQLException {
        // Check cache first
        if (documentTextCache.containsKey(documentId)) {
            return documentTextCache.get(documentId);
        }
        
        // If not in cache, fetch from database
        String sql = "SELECT text FROM documents WHERE document_id = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, documentId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String text = rs.getString("text");
                    
                    // Cache the result for future use
                    documentTextCache.put(documentId, text);
                    
                    return text;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Extracts a snippet from document text with context around a token.
     * 
     * @param documentText The full document text
     * @param tokenBegin Start position of token
     * @param tokenEnd End position of token
     * @param token The token text
     * @param variableName The variable name for logging
     * @return A formatted snippet with context
     */
    private String extractContextSnippet(
            String documentText, int tokenBegin, int tokenEnd, 
            String token, String variableName) {
        
        if (tokenBegin < 0 || tokenEnd > documentText.length() || tokenBegin >= tokenEnd) {
            logger.warn("Invalid token positions: begin={}, end={}, docLength={}", 
                tokenBegin, tokenEnd, documentText.length());
            return documentText.substring(
                Math.max(0, Math.min(documentText.length() - 1, tokenBegin) - 50),
                Math.min(documentText.length(), Math.max(0, tokenEnd) + 50)
            );
        }
        
        // Calculate context window (character-based)
        int contextSize = windowSize * 30; // Approximate characters per token
        int snippetStart = Math.max(0, tokenBegin - contextSize);
        int snippetEnd = Math.min(documentText.length(), tokenEnd + contextSize);
        
        // Get text with context
        String snippetText = documentText.substring(snippetStart, snippetEnd);
        
        // Adjust token position in snippet
        int highlightStart = tokenBegin - snippetStart;
        int highlightEnd = tokenEnd - snippetStart;
        
        // Add sentence boundaries if requested
        if (showSentenceBoundaries) {
            snippetText = "[...] " + snippetText + " [...]";
            highlightStart += 6;
            highlightEnd += 6;
        }
        
        // Apply highlighting
        StringBuilder result = new StringBuilder(snippetText);
        result.insert(highlightEnd, highlightStyle);
        result.insert(highlightStart, highlightStyle);
        
        logger.debug("Created snippet for {}: {}", variableName, 
            result.length() > 50 ? result.substring(0, 50) + "..." : result);
        
        return result.toString();
    }
    
    /**
     * Clears the document text cache to save memory.
     * Should be called after processing a batch of results.
     */
    public void clearCache() {
        documentTextCache.clear();
        logger.debug("Cleared document text cache");
    }
    
    /**
     * Internal class to hold information about a token match.
     */
    private static class MatchInfo {
        final String token;
        final int beginPos;
        final int endPos;
        final String matchedText;
        
        MatchInfo(String token, int beginPos, int endPos, String matchedText) {
            this.token = token;
            this.beginPos = beginPos;
            this.endPos = endPos;
            this.matchedText = matchedText;
        }
    }
} 
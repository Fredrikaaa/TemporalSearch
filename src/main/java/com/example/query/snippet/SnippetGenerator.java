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
     * Generates a snippet of text around a token in a document.
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
        logger.debug("Using backward compatibility method for documentId={}, sentenceId={}, tokenPosition={}, variableName={}",
            documentId, sentenceId, tokenPosition, variableName);
        
        // Get document text to find the token
        String documentText = getDocumentText(documentId);
        if (documentText == null || documentText.isEmpty()) {
            logger.warn("Document text not found: documentId={}", documentId);
            return "[Document text not found]";
        }
        
        // Find the token based on position
        if (tokenPosition < 0 || tokenPosition >= documentText.length()) {
            logger.warn("Token position {} is out of bounds", tokenPosition);
            return generatePositionBasedSnippet(documentText, tokenPosition, variableName);
        }
        
        // For backward compatibility, we'll expand around the token position to create a reasonable highlight
        int beginPos = tokenPosition;
        
        // Search backward to find the beginning of the word
        while (beginPos > 0 && !Character.isWhitespace(documentText.charAt(beginPos - 1))) {
            beginPos--;
        }
        
        // Search forward to find the end of the word
        int endPos = tokenPosition;
        while (endPos < documentText.length() && !Character.isWhitespace(documentText.charAt(endPos))) {
            endPos++;
        }
        
        // Use the new method with the calculated positions
        return generateSnippet(documentId, sentenceId, beginPos, endPos, variableName);
    }
    
    /**
     * Generates a snippet of text around a variable match.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID where the match occurs
     * @param beginPos The beginning character position of the match
     * @param endPos The ending character position of the match
     * @param variableName The name of the variable that matched
     * @return Formatted snippet text with highlights
     * @throws SQLException if a database error occurs
     */
    public String generateSnippet(long documentId, int sentenceId, int beginPos, int endPos, String variableName) 
            throws SQLException {
        logger.debug("Generating snippet for documentId={}, sentenceId={}, span=[{},{}], variable={}",
            documentId, sentenceId, beginPos, endPos, variableName);
        
        // Get document text (using cache if available)
        String documentText = getDocumentText(documentId);
        if (documentText == null || documentText.isEmpty()) {
            logger.warn("Document text not found for document {}", documentId);
            return "[Document text not found]";
        }
        
        // Verify positions are valid
        if (beginPos < 0 || endPos < 0 || beginPos >= documentText.length() || endPos > documentText.length() || beginPos >= endPos) {
            logger.warn("Invalid positions ({}, {}) for document {} with length {}", 
                beginPos, endPos, documentId, documentText.length());
            return "[Invalid position]";
        }
        
        // Extract the actual entity text from the document
        String matchedText = documentText.substring(beginPos, endPos);
        
        // Create the snippet using the extracted text
        String snippet = extractContextSnippet(documentText, beginPos, endPos, matchedText, variableName);
        
        return snippet;
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
        int contextChars = 75;
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
        
        // First try to find a named entity (NER) at the position
        MatchInfo nerEntity = findNamedEntityAtPosition(documentId, sentenceId, position);
        if (nerEntity != null) {
            return nerEntity;
        }
        
        // If no named entity found, fall back to finding the nearest token
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
     * Attempts to find a complete named entity at the given position.
     * Specifically looks for NER-annotated tokens.
     *
     * @param documentId The document ID
     * @param sentenceId The sentence ID
     * @param position The character position to search at
     * @return The entity information if found, or null if no entity at this position
     * @throws SQLException if a database error occurs
     */
    private MatchInfo findNamedEntityAtPosition(long documentId, int sentenceId, int position) throws SQLException {
        String sql = """
            WITH tokens AS (
                SELECT 
                    document_id, 
                    sentence_id, 
                    begin_char, 
                    end_char, 
                    token,
                    ner,
                    ROW_NUMBER() OVER (ORDER BY begin_char) as row_num
                FROM annotations
                WHERE document_id = ?
                  AND (sentence_id = ? OR ? = -1)
                  AND ner IS NOT NULL 
                  AND ner != '' 
                  AND ner != 'O'
                ORDER BY begin_char
            ),
            -- Identify entity group boundaries based on position and same NER type
            entity_groups AS (
                SELECT 
                    t1.row_num,
                    t1.document_id,
                    t1.sentence_id,
                    t1.ner,
                    -- Create entity group ID based on gaps in sequence and changes in NER type
                    SUM(CASE 
                        WHEN t1.row_num = 1 THEN 1
                        WHEN t1.ner != LAG(t1.ner) OVER (ORDER BY t1.row_num) THEN 1
                        WHEN t1.begin_char > LAG(t1.end_char) OVER (ORDER BY t1.row_num) + 3 THEN 1
                        ELSE 0
                    END) OVER (ORDER BY t1.row_num) as entity_group_id
                FROM tokens t1
            ),
            -- Calculate entity boundaries and text for each entity group
            entity_boundaries AS (
                SELECT 
                    eg.entity_group_id,
                    eg.document_id,
                    eg.sentence_id,
                    eg.ner,
                    MIN(t.begin_char) as entity_begin,
                    MAX(t.end_char) as entity_end,
                    GROUP_CONCAT(t.token, ' ') as entity_text
                FROM entity_groups eg
                JOIN tokens t ON eg.row_num = t.row_num
                GROUP BY eg.entity_group_id, eg.document_id, eg.sentence_id, eg.ner
            )
            -- Find the entity containing the position
            SELECT 
                entity_begin, 
                entity_end, 
                entity_text,
                ner
            FROM entity_boundaries
            WHERE ? BETWEEN entity_begin AND entity_end
            LIMIT 1
            """;
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, documentId);
            stmt.setInt(2, sentenceId);
            stmt.setInt(3, sentenceId);
            stmt.setInt(4, position);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int beginChar = rs.getInt("entity_begin");
                    int endChar = rs.getInt("entity_end");
                    String entityText = rs.getString("entity_text");
                    String nerType = rs.getString("ner");
                    
                    logger.debug("Found NER entity '{}' type={} at position [{},{}]", 
                        entityText, nerType, beginChar, endChar);
                    
                    return new MatchInfo(
                        entityText,
                        beginChar,
                        endChar,
                        entityText
                    );
                }
            }
        } catch (SQLException e) {
            // If the advanced SQL fails (e.g., window functions not supported), try a simpler approach
            logger.debug("Advanced entity query failed: {}", e.getMessage());
            
            // Simple query to just get the token at the exact position
            String simpleQuery = """
                SELECT begin_char, end_char, token, ner
                FROM annotations
                WHERE document_id = ?
                  AND (sentence_id = ? OR ? = -1)
                  AND ? BETWEEN begin_char AND end_char
                LIMIT 1
                """;
                
            try (PreparedStatement stmt = connection.prepareStatement(simpleQuery)) {
                stmt.setLong(1, documentId);
                stmt.setInt(2, sentenceId);
                stmt.setInt(3, sentenceId);
                stmt.setInt(4, position);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int beginChar = rs.getInt("begin_char");
                        int endChar = rs.getInt("end_char");
                        String token = rs.getString("token");
                        
                        return new MatchInfo(
                            token,
                            beginChar,
                            endChar,
                            token
                        );
                    }
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
            // Use a fixed fallback size for invalid positions
            int fallbackContextSize = 75;
            return documentText.substring(
                Math.max(0, Math.min(documentText.length() - 1, tokenBegin) - fallbackContextSize),
                Math.min(documentText.length(), Math.max(0, tokenEnd) + fallbackContextSize)
            );
        }
        
        // Calculate context size based only on window size
        // We delegate formatting concerns completely to ResultFormatter
        int contextSize = windowSize * 25; // Characters per window size unit (reduced from 35)
        
        // For bigger window sizes, add extra characters to avoid truncation
        if (windowSize >= 3) {
            contextSize = windowSize * 30; // Reduced from 50
        }
        
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
        
        logger.debug("Created snippet for {}: {} chars", variableName, result.length());
        
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
        
        String getToken() {
            return token;
        }
        
        int getBeginChar() {
            return beginPos;
        }
        
        int getEndChar() {
            return endPos;
        }
    }
} 
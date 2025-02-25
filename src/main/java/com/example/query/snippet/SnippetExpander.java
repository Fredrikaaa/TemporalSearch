package com.example.query.snippet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expands context around an anchor point by retrieving surrounding sentences
 */
public class SnippetExpander {
    private static final Logger logger = LoggerFactory.getLogger(SnippetExpander.class);
    private final Connection connection;

    /**
     * Creates a new SnippetExpander with the given database connection
     * @param connection Database connection to use for retrieving sentences
     */
    public SnippetExpander(Connection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("connection must not be null");
        }
        this.connection = connection;
    }

    /**
     * Expands context around an anchor point by retrieving surrounding sentences
     * @param anchor The anchor point to expand around
     * @param windowSize Number of sentences to include on each side
     * @return List of sentences in the context window
     * @throws SQLException if a database access error occurs
     */
    public List<SentenceContext> expand(ContextAnchor anchor, int windowSize) throws SQLException {
        logger.debug("Expanding context around anchor: documentId={}, sentenceId={}, windowSize={}", 
            anchor.documentId(), anchor.sentenceId(), windowSize);
            
        // Calculate start and end sentence IDs
        int startSentenceId = Math.max(0, anchor.sentenceId() - windowSize);
        int endSentenceId = anchor.sentenceId() + windowSize;

        logger.debug("Sentence window: {} to {}", startSentenceId, endSentenceId);

        // Since we don't have a dedicated sentences table, we need to reconstruct sentences from annotations
        // First, get all tokens for the sentences in the window
        String sql = """
            SELECT DISTINCT sentence_id, token, begin_char, end_char
            FROM annotations
            WHERE document_id = ? AND sentence_id BETWEEN ? AND ?
            ORDER BY sentence_id, begin_char
            """;

        List<SentenceContext> sentences = new ArrayList<>();
        int currentSentenceId = -1;
        StringBuilder currentSentence = new StringBuilder();
        int sentenceBeginChar = -1;
        int sentenceEndChar = -1;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, anchor.documentId());
            stmt.setInt(2, startSentenceId);
            stmt.setInt(3, endSentenceId);

            logger.debug("Executing query: {}", sql);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    int sentenceId = rs.getInt("sentence_id");
                    String token = rs.getString("token");
                    int beginChar = rs.getInt("begin_char");
                    int endChar = rs.getInt("end_char");
                    
                    // If we've moved to a new sentence, store the previous one
                    if (sentenceId != currentSentenceId) {
                        if (currentSentenceId != -1) {
                            sentences.add(new SentenceContext(
                                currentSentenceId,
                                currentSentence.toString().trim(),
                                sentenceBeginChar,
                                sentenceEndChar,
                                anchor.sentenceId() == currentSentenceId
                            ));
                        }
                        
                        // Start a new sentence
                        currentSentenceId = sentenceId;
                        currentSentence = new StringBuilder();
                        sentenceBeginChar = beginChar;
                    }
                    
                    // Add token to current sentence with appropriate spacing
                    if (currentSentence.length() > 0 && !token.startsWith(".") && !token.startsWith(",")) {
                        currentSentence.append(" ");
                    }
                    currentSentence.append(token);
                    sentenceEndChar = endChar;
                }
                
                // Add the last sentence if there was one
                if (currentSentenceId != -1) {
                    sentences.add(new SentenceContext(
                        currentSentenceId,
                        currentSentence.toString().trim(),
                        sentenceBeginChar,
                        sentenceEndChar,
                        anchor.sentenceId() == currentSentenceId
                    ));
                }
            }
        }

        logger.debug("Retrieved {} sentences for context window", sentences.size());
        return sentences;
    }

    /**
     * Represents a sentence in the context window
     */
    public record SentenceContext(
        int sentenceId,
        String text,
        int beginChar,
        int endChar,
        boolean isMatchSentence
    ) {}
} 
package com.example.query.model.document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Provides access to document text stored in SQLite.
 */
public class DocumentStore {
    private static final Logger logger = Logger.getLogger(DocumentStore.class.getName());
    private static final String SELECT_TEXT = "SELECT text FROM documents WHERE id = ?";
    private static final String SELECT_BOUNDARIES = 
        "SELECT text FROM documents WHERE id = ? AND ? >= text_start AND ? <= text_end";

    private final Connection connection;

    /**
     * Creates a new document store.
     * @param connection The SQLite database connection
     */
    public DocumentStore(Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets the text for a document.
     * @param docId The document ID
     * @return The document text, or null if not found
     */
    public String getText(int docId) {
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_TEXT)) {
            stmt.setInt(1, docId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("text");
                }
            }
        } catch (SQLException e) {
            logger.warning("Failed to get text for document " + docId + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Gets a snippet of text from a document.
     * @param docId The document ID
     * @param start The start position
     * @param end The end position
     * @return The text snippet, or null if not found
     */
    public String getSnippet(int docId, int start, int end) {
        String text = getText(docId);
        if (text == null) {
            return null;
        }

        // Validate boundaries
        if (start < 0) {
            start = 0;
        }
        if (end > text.length()) {
            end = text.length();
        }
        if (start >= end || start >= text.length()) {
            return "";
        }

        return text.substring(start, end);
    }

    /**
     * Gets text boundaries for a document.
     * @param docId The document ID
     * @param position The position to check
     * @return The text boundaries containing the position, or null if not found
     */
    public TextBoundary getBoundaries(int docId, int position) {
        try (PreparedStatement stmt = connection.prepareStatement(SELECT_BOUNDARIES)) {
            stmt.setInt(1, docId);
            stmt.setInt(2, position);
            stmt.setInt(3, position);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new TextBoundary(
                        rs.getInt("text_start"),
                        rs.getInt("text_end")
                    );
                }
            }
        } catch (SQLException e) {
            logger.warning("Failed to get boundaries for document " + docId + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Gets text boundaries for a document using sentence boundaries.
     * @param docId The document ID
     * @param position The position to check
     * @return The sentence boundaries containing the position, or null if not found
     */
    public TextBoundary getSentenceBoundaries(int docId, int position) {
        String sql = """
            SELECT MIN(begin_char) as start_char, MAX(end_char) as end_char
            FROM annotations
            WHERE document_id = ?
            AND sentence_id = (
                SELECT sentence_id 
                FROM annotations 
                WHERE document_id = ? 
                AND begin_char <= ? 
                AND end_char > ?
                LIMIT 1
            )
        """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, docId);
            stmt.setInt(2, docId);
            stmt.setInt(3, position);
            stmt.setInt(4, position);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int start = rs.getInt("start_char");
                    int end = rs.getInt("end_char");
                    if (!rs.wasNull() && start >= 0 && end > start) {
                        return new TextBoundary(start, end);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warning("Failed to get sentence boundaries for document " + docId + ": " + e.getMessage());
        }
        return null;
    }
} 
package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.*;

/**
 * Generates bigram indexes from annotated text.
 * Each bigram (pair of consecutive words) is stored with its positions in the corpus.
 */
public class BigramIndexGenerator extends BaseIndexGenerator<AnnotationEntry> {
    
    public BigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations");
    }

    public BigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations", threadCount);
    }

    @Override
    protected List<AnnotationEntry> fetchBatch(int offset) throws SQLException {
        List<AnnotationEntry> entries = new ArrayList<>();
        try (Statement stmt = sqliteConn.createStatement()) {
            // First get document timestamps
            Map<Integer, LocalDate> documentDates = new HashMap<>();
            try (ResultSet rs = stmt.executeQuery("SELECT document_id, timestamp FROM documents")) {
                while (rs.next()) {
                    documentDates.put(
                        rs.getInt("document_id"),
                        ZonedDateTime.parse(rs.getString("timestamp")).toLocalDate()
                    );
                }
            }
            
            // Get consecutive words
            String query = String.format("""
                SELECT a1.*, a2.lemma as next_lemma, a2.begin_char as next_begin, a2.end_char as next_end
                FROM annotations a1
                LEFT JOIN annotations a2 ON a1.document_id = a2.document_id 
                    AND a1.sentence_id = a2.sentence_id
                    AND a2.begin_char > a1.end_char
                    AND NOT EXISTS (
                        SELECT 1 FROM annotations a3
                        WHERE a3.document_id = a1.document_id
                            AND a3.sentence_id = a1.sentence_id
                            AND a3.begin_char > a1.end_char
                            AND a3.begin_char < a2.begin_char
                    )
                ORDER BY a1.document_id, a1.sentence_id, a1.begin_char
                LIMIT %d OFFSET %d
                """, batchSize, offset);
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    int docId = rs.getInt("document_id");
                    LocalDate timestamp = documentDates.get(docId);
                    if (timestamp == null) {
                        throw new SQLException("Document " + docId + " not found in documents table");
                    }

                    // Sanitize text fields
                    String firstWord = sanitizeText(rs.getString("lemma"));
                    String secondWord = sanitizeText(rs.getString("next_lemma"));
                    
                    // Skip if either word is null or empty after sanitization
                    if (firstWord == null || firstWord.isEmpty() ||
                        secondWord == null || secondWord.isEmpty()) {
                        continue;
                    }
                    
                    // Create an entry for the bigram
                    // We store both words in the lemma field, separated by a delimiter
                    entries.add(new AnnotationEntry(
                        docId,
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("next_end"),
                        firstWord + DELIMITER + secondWord,
                        rs.getString("pos"), // Original POS tag of first word
                        timestamp
                    ));
                }
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<AnnotationEntry> partition) throws IOException {
        ListMultimap<String, PositionList> results = MultimapBuilder.hashKeys().arrayListValues().build();
        
        for (AnnotationEntry entry : partition) {
            // Split the combined lemma to get both words
            String[] words = entry.getLemma().split(DELIMITER);
            if (words.length != 2) {
                continue; // Skip malformed entries
            }
            
            // Create key in format: firstWord\0secondWord
            String key = words[0].toLowerCase() + DELIMITER + words[1].toLowerCase();
            
            // Create position list for this entry
            PositionList positions = new PositionList();
            positions.add(new Position(
                entry.getDocumentId(),
                entry.getSentenceId(),
                entry.getBeginChar(),
                entry.getEndChar(),
                entry.getTimestamp()
            ));
            
            results.put(key, positions);
        }
        
        return results;
    }
}

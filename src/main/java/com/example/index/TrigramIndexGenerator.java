package com.example.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.example.logging.ProgressTracker;

/**
 * Generates a streaming trigram index from annotation entries.
 * Each entry maps a sequence of three consecutive lemmatized tokens to their positions in the corpus.
 * Uses streaming processing and external sorting for efficient memory usage.
 */
public final class TrigramIndexGenerator extends IndexGenerator<AnnotationEntry> {
    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, sqliteConn, progress);
    }

    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress, IndexConfig config) throws IOException {
        super(levelDbPath, stopwordsPath, sqliteConn, progress, config);
    }

    @Override
    protected List<AnnotationEntry> fetchBatch(int offset) throws SQLException {
        List<AnnotationEntry> batch = new ArrayList<>();
        String query = "SELECT a.document_id, a.sentence_id, a.begin_char, a.end_char, a.lemma, a.pos, d.timestamp " +
                      "FROM annotations a " +
                      "JOIN documents d ON a.document_id = d.document_id " +
                      "ORDER BY a.document_id, a.sentence_id, a.begin_char LIMIT ? OFFSET ?";
        
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
            stmt.setInt(1, DOC_BATCH_SIZE);
            stmt.setInt(2, offset);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    AnnotationEntry entry = new AnnotationEntry(
                        rs.getInt("document_id"),
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        rs.getString("lemma"),
                        rs.getString("pos"),
                        LocalDate.parse(rs.getString("timestamp").substring(0, 10))
                    );
                    batch.add(entry);
                }
            }
        }
        
        return batch;
    }

    @Override
    protected ListMultimap<String, PositionList> processBatch(List<AnnotationEntry> batch) throws IOException {
        ListMultimap<String, PositionList> index = ArrayListMultimap.create();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (int i = 0; i < batch.size() - 2; i++) {
            AnnotationEntry entry = batch.get(i);
            AnnotationEntry nextEntry = batch.get(i + 1);
            AnnotationEntry nextNextEntry = batch.get(i + 2);
            
            // Skip if any entry has null lemma
            if (entry.getLemma() == null || nextEntry.getLemma() == null || nextNextEntry.getLemma() == null) {
                continue;
            }
            
            // Check if all entries are from the same document and sentence
            if (entry.getDocumentId() == nextEntry.getDocumentId() && 
                entry.getDocumentId() == nextNextEntry.getDocumentId() &&
                entry.getSentenceId() == nextEntry.getSentenceId() &&
                entry.getSentenceId() == nextNextEntry.getSentenceId()) {
                
                String key = String.format("%s%s%s%s%s", 
                    entry.getLemma().toLowerCase(),
                    DELIMITER,
                    nextEntry.getLemma().toLowerCase(),
                    DELIMITER,
                    nextNextEntry.getLemma().toLowerCase());

                Position position = new Position(nextNextEntry.getDocumentId(), nextNextEntry.getSentenceId(),
                    entry.getBeginChar(), nextNextEntry.getEndChar(), nextNextEntry.getTimestamp());

                // Get or create position list for this trigram
                PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
                posList.add(position);
            }
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }

    @Override
    protected String getTableName() {
        return "annotations";
    }

    @Override
    protected String getIndexName() {
        return "trigram";
    }
} 
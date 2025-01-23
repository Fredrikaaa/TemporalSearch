package com.example.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.example.logging.ProgressTracker;

/**
 * Generates a unigram index from annotation entries.
 * Each entry maps a single lemmatized token to its positions in the corpus.
 */
public final class UnigramIndexGenerator extends BaseIndexGenerator<AnnotationEntry> {

    public UnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations");
    }

    public UnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations", 
              Runtime.getRuntime().availableProcessors(), progress);
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
            
            ResultSet rs = stmt.executeQuery(
                String.format("SELECT * FROM annotations ORDER BY document_id, sentence_id, begin_char LIMIT %d OFFSET %d",
                    batchSize, offset)
            );
            
            while (rs.next()) {
                int docId = rs.getInt("document_id");
                LocalDate timestamp = documentDates.get(docId);
                if (timestamp == null) {
                    throw new SQLException("Document " + docId + " not found in documents table");
                }

                // Sanitize text fields before creating entry
                String lemma = sanitizeText(rs.getString("lemma"));
                String pos = sanitizeText(rs.getString("pos"));

                // Skip if lemma is null or empty after sanitization
                if (lemma == null || lemma.isEmpty()) {
                    continue;
                }
                
                entries.add(new AnnotationEntry(
                    docId,
                    rs.getInt("sentence_id"),
                    rs.getInt("begin_char"),
                    rs.getInt("end_char"),
                    lemma,
                    pos,
                    timestamp
                ));
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<AnnotationEntry> partition) {
        ListMultimap<String, PositionList> index = ArrayListMultimap.create();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (AnnotationEntry entry : partition) {
            // Skip entries with null or empty lemmas
            if (entry.getLemma() == null || entry.getLemma().trim().isEmpty()) {
                continue;
            }
            
            // Convert lemma to lowercase for consistency
            String key = entry.getLemma().toLowerCase().trim();
            
            // Skip stopwords
            if (isStopword(key)) {
                continue;
            }
            
            Position position = new Position(entry.getDocumentId(), entry.getSentenceId(),
                entry.getBeginChar(), entry.getEndChar(), entry.getTimestamp());
            
            // Get or create position list for this lemma
            PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
            posList.add(position);
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }
}

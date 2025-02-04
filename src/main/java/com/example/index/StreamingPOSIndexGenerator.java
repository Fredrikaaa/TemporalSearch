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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;

/**
 * Generates a streaming POS (Part-of-Speech) index from annotation entries.
 * Each entry maps a POS tag to its positions in the corpus.
 * Uses streaming processing and external sorting for efficient memory usage.
 */
public final class StreamingPOSIndexGenerator extends IndexGenerator<AnnotationEntry> {
    private static final Logger logger = LoggerFactory.getLogger(StreamingPOSIndexGenerator.class);

    public StreamingPOSIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, sqliteConn, progress);
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
        
        for (AnnotationEntry entry : batch) {
            // Skip entries with null or empty POS tags
            if (entry.getPos() == null || entry.getPos().trim().isEmpty()) {
                continue;
            }
            
            // Convert POS tag to lowercase for consistency
            String key = entry.getPos().toLowerCase().trim();
            
            Position position = new Position(entry.getDocumentId(), entry.getSentenceId(),
                entry.getBeginChar(), entry.getEndChar(), entry.getTimestamp());
            
            // Get or create position list for this POS tag
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
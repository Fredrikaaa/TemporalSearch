package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import com.example.logging.ProgressTracker;

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
            int batchSize, Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations", 
              Runtime.getRuntime().availableProcessors(), progress);
    }

    @Override
    protected List<AnnotationEntry> fetchBatch(int offset) throws SQLException {
        List<AnnotationEntry> entries = new ArrayList<>();
        String sql = "SELECT a.document_id, a.sentence_id, a.begin_char, a.end_char, a.lemma, d.timestamp " +
                    "FROM annotations a " +
                    "JOIN documents d ON a.document_id = d.document_id " +
                    "WHERE a.lemma IS NOT NULL " +
                    "ORDER BY a.document_id, a.sentence_id, a.begin_char LIMIT ? OFFSET ?";
                    
        try (PreparedStatement stmt = sqliteConn.prepareStatement(sql)) {
            stmt.setInt(1, batchSize);
            stmt.setInt(2, offset);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    entries.add(new AnnotationEntry(
                        rs.getInt("document_id"),
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        sanitizeText(rs.getString("lemma")),
                        "", // POS tag not needed for bigrams
                        LocalDate.parse(rs.getString("timestamp").substring(0, 10))
                    ));
                }
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<AnnotationEntry> partition) throws IOException {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (int i = 0; i < partition.size() - 1; i++) {
            AnnotationEntry entry = partition.get(i);
            AnnotationEntry nextEntry = partition.get(i + 1);
            
            // Skip if either word is null
            if (entry.getLemma() == null || nextEntry.getLemma() == null) {
                continue;
            }
            
            // Check if entries are from the same document and sentence
            if (entry.getDocumentId() == nextEntry.getDocumentId() && 
                entry.getSentenceId() == nextEntry.getSentenceId()) {
                
                String key = String.format("%s%s%s", 
                    entry.getLemma().toLowerCase(),
                    BaseIndexGenerator.DELIMITER,
                    nextEntry.getLemma().toLowerCase());

                Position position = new Position(nextEntry.getDocumentId(), nextEntry.getSentenceId(),
                    entry.getBeginChar(), nextEntry.getEndChar(), nextEntry.getTimestamp());

                // Get or create position list for this bigram
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
}

package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Parallel implementation of unigram index generation.
 * Processes documents in parallel to create a unigram index.
 */
public class ParallelUnigramIndexGenerator extends ParallelIndexGenerator<AnnotationEntry> {
    
    public ParallelUnigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "unigrams", threadCount);
    }

    @Override
    protected List<AnnotationEntry> fetchBatch(int offset) throws SQLException {
        List<AnnotationEntry> entries = new ArrayList<>();
        String query = """
            SELECT document_id, sentence_id, begin_char, end_char, timestamp, lemma, pos
            FROM annotations
            ORDER BY document_id, sentence_id, begin_char
            LIMIT ? OFFSET ?
            """;
            
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
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
                        sanitizeText(rs.getString("pos")),
                        LocalDate.parse(rs.getString("timestamp"))
                    ));
                }
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<AnnotationEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        
        for (AnnotationEntry entry : partition) {
            if (entry.getLemma() == null || isStopword(entry.getLemma())) {
                continue;
            }

            String key = entry.getLemma().toLowerCase();
            Position position = new Position(entry.getDocumentId(), entry.getSentenceId(), 
                                          entry.getBeginChar(), entry.getEndChar(), entry.getTimestamp());

            // Get or create position list for this term
            List<PositionList> lists = index.get(key);
            if (lists.isEmpty()) {
                PositionList newList = new PositionList();
                newList.add(position);
                index.put(key, newList);
            } else {
                lists.get(0).add(position);
            }
        }
        
        return index;
    }
} 
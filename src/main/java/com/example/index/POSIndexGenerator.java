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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;

/**
 * Generates indexes for part-of-speech tags from annotated text.
 * Each POS tag is stored with its positions in the corpus.
 * No stopword filtering is applied as all POS tags are considered significant.
 */
public class POSIndexGenerator extends BaseIndexGenerator<POSIndexGenerator.POSEntry> {
    private static final Logger logger = LoggerFactory.getLogger(POSIndexGenerator.class);
    
    public static class POSEntry implements IndexEntry {
        private int documentId;
        private int sentenceId;
        private int beginChar;
        private int endChar;
        private String pos;
        private LocalDate timestamp;
        
        @Override
        public int getDocumentId() {
            return documentId;
        }
        
        @Override
        public int getSentenceId() {
            return sentenceId;
        }
        
        @Override
        public int getBeginChar() {
            return beginChar;
        }
        
        @Override
        public int getEndChar() {
            return endChar;
        }
        
        @Override
        public LocalDate getTimestamp() {
            return timestamp;
        }
    }
    
    public POSIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations");
    }

    public POSIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations", 
              Runtime.getRuntime().availableProcessors(), progress);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<POSEntry> partition) throws IOException {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (POSEntry entry : partition) {
            // Skip entries with null or empty POS tags
            if (entry.pos == null || entry.pos.trim().isEmpty()) {
                continue;
            }
            
            // Convert POS tag to lowercase for consistency
            String key = entry.pos.toLowerCase().trim();
            
            Position position = new Position(entry.documentId, entry.sentenceId,
                entry.beginChar, entry.endChar, entry.timestamp);

            // Get or create position list for this POS tag
            PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
            posList.add(position);
            
            logger.debug("Added POS tag: {} at position {}", key, position);
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }

    @Override
    protected List<POSEntry> fetchBatch(int offset) throws SQLException {
        List<POSEntry> entries = new ArrayList<>();
        String sql = "SELECT a.document_id, a.sentence_id, a.begin_char, a.end_char, a.pos, d.timestamp " +
                    "FROM annotations a " +
                    "JOIN documents d ON a.document_id = d.document_id " +
                    "WHERE a.pos IS NOT NULL " +
                    "ORDER BY a.document_id, a.sentence_id, a.begin_char LIMIT ? OFFSET ?";
                    
        try (PreparedStatement stmt = sqliteConn.prepareStatement(sql)) {
            stmt.setInt(1, batchSize);
            stmt.setInt(2, offset);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    POSEntry entry = new POSEntry();
                    entry.documentId = rs.getInt("document_id");
                    entry.sentenceId = rs.getInt("sentence_id");
                    entry.beginChar = rs.getInt("begin_char");
                    entry.endChar = rs.getInt("end_char");
                    entry.pos = rs.getString("pos");
                    entry.timestamp = LocalDate.parse(rs.getString("timestamp").substring(0, 10));
                    entries.add(entry);
                }
            }
        }
        return entries;
    }
} 
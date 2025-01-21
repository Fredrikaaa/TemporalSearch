package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Generates trigram indexes from annotated text.
 * Each trigram (sequence of three consecutive words) is stored with its positions in the corpus.
 */
public class TrigramIndexGenerator extends BaseIndexGenerator<TrigramIndexGenerator.TrigramEntry> {
    
    public static class TrigramEntry implements IndexEntry {
        private int documentId;
        private int sentenceId;
        private int beginChar;
        private int endChar;
        private String lemma;
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
    
    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations");
    }

    public TrigramIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations", threadCount);
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<TrigramEntry> partition) throws IOException {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (int i = 0; i < partition.size() - 2; i++) {
            TrigramEntry entry = partition.get(i);
            TrigramEntry nextEntry = partition.get(i + 1);
            TrigramEntry nextNextEntry = partition.get(i + 2);
            
            // Skip if any entry has null lemma
            if (entry.lemma == null || nextEntry.lemma == null || nextNextEntry.lemma == null) {
                continue;
            }
            
            // Check if all entries are from the same document and sentence
            if (entry.documentId == nextEntry.documentId && 
                entry.documentId == nextNextEntry.documentId &&
                entry.sentenceId == nextEntry.sentenceId &&
                entry.sentenceId == nextNextEntry.sentenceId) {
                
                String key = String.format("%s%s%s%s%s", 
                    entry.lemma.toLowerCase(),
                    BaseIndexGenerator.DELIMITER,
                    nextEntry.lemma.toLowerCase(),
                    BaseIndexGenerator.DELIMITER,
                    nextNextEntry.lemma.toLowerCase());

                Position position = new Position(nextNextEntry.documentId, nextNextEntry.sentenceId,
                    entry.beginChar, nextNextEntry.endChar, nextNextEntry.timestamp);

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
    protected List<TrigramEntry> fetchBatch(int offset) throws SQLException {
        List<TrigramEntry> entries = new ArrayList<>();
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
                    TrigramEntry entry = new TrigramEntry();
                    entry.documentId = rs.getInt("document_id");
                    entry.sentenceId = rs.getInt("sentence_id");
                    entry.beginChar = rs.getInt("begin_char");
                    entry.endChar = rs.getInt("end_char");
                    entry.lemma = rs.getString("lemma");
                    entry.timestamp = LocalDate.parse(rs.getString("timestamp").substring(0, 10));
                    entries.add(entry);
                }
            }
        }
        return entries;
    }
}

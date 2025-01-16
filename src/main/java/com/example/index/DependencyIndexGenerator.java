package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import me.tongfei.progressbar.ProgressBar;
import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZonedDateTime;

public class DependencyIndexGenerator extends BaseIndexGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DependencyIndexGenerator.class);
    private static final int BATCH_SIZE = 1000;

    private static final Set<String> BLACKLISTED_RELATIONS = new HashSet<>(Arrays.asList(
        "punct", "det", "case", "cc"
    ));

    private static class DependencyEntry {
        private final int documentId;
        private final int sentenceId;
        private final String headToken;
        private final String dependentToken;
        private final String relation;
        private final int beginChar;
        private final int endChar;

        public DependencyEntry(int documentId, int sentenceId, String headToken, 
                             String dependentToken, String relation, int beginChar, int endChar) {
            this.documentId = documentId;
            this.sentenceId = sentenceId;
            this.headToken = headToken;
            this.dependentToken = dependentToken;
            this.relation = relation;
            this.beginChar = beginChar;
            this.endChar = endChar;
        }
    }

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies");
    }

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies", threadCount);
    }

    @Override
    protected List<IndexEntry> fetchBatch(int offset) throws SQLException {
        List<IndexEntry> entries = new ArrayList<>();
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
            
            // Get dependencies
            String query = String.format("""
                SELECT * FROM dependencies
                ORDER BY document_id, sentence_id
                LIMIT %d OFFSET %d
                """, batchSize, offset);
            
            logger.debug("Fetching batch at offset {} with query: {}", offset, query);
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    int docId = rs.getInt("document_id");
                    LocalDate timestamp = documentDates.get(docId);
                    if (timestamp == null) {
                        throw new SQLException("Document " + docId + " not found in documents table");
                    }
                    
                    // Skip blacklisted relations
                    String relation = rs.getString("relation");
                    if (BLACKLISTED_RELATIONS.contains(relation)) {
                        continue;
                    }
                    
                    // Create an entry for the head token
                    entries.add(new IndexEntry(
                        docId,
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        rs.getString("head_token"),
                        relation, // Use relation as POS tag for head
                        timestamp
                    ));
                    
                    // Create an entry for the dependent token
                    entries.add(new IndexEntry(
                        docId,
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        rs.getString("dependent_token"),
                        relation, // Use relation as POS tag for dependent
                        timestamp
                    ));
                }
            }
            
            logger.debug("Fetched {} entries at offset {}", entries.size(), offset);
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> entries) {
        ListMultimap<String, PositionList> result = ArrayListMultimap.create();
        Map<String, PositionList> positionLists = new HashMap<>();

        // Group entries by document and sentence
        Map<Integer, Map<Integer, List<IndexEntry>>> docSentMap = new HashMap<>();
        for (IndexEntry entry : entries) {
            docSentMap
                .computeIfAbsent(entry.documentId, k -> new HashMap<>())
                .computeIfAbsent(entry.sentenceId, k -> new ArrayList<>())
                .add(entry);
        }

        // Process each document
        for (Map.Entry<Integer, Map<Integer, List<IndexEntry>>> docEntry : docSentMap.entrySet()) {
            int docId = docEntry.getKey();

            // Process each sentence
            for (Map.Entry<Integer, List<IndexEntry>> sentEntry : docEntry.getValue().entrySet()) {
                int sentId = sentEntry.getKey();
                List<IndexEntry> sentEntries = sentEntry.getValue();
                
                // Process entries in pairs (head and dependent)
                for (int i = 0; i < sentEntries.size(); i += 2) {
                    if (i + 1 >= sentEntries.size()) {
                        break; // Skip incomplete pair at end
                    }
                    
                    IndexEntry headEntry = sentEntries.get(i);
                    IndexEntry depEntry = sentEntries.get(i + 1);
                    
                    // Skip if either word is a stopword
                    if (isStopword(headEntry.lemma) || isStopword(depEntry.lemma)) {
                        continue;
                    }
                    
                    // Create the key with the relation type
                    String key = String.format("%s\u0000%s\u0000%s",
                        headEntry.lemma.toLowerCase(),
                        headEntry.pos, // relation type stored in pos field
                        depEntry.lemma.toLowerCase());
                    
                    // Use the minimum and maximum character positions to span both tokens
                    int beginPos = Math.min(headEntry.beginChar, depEntry.beginChar);
                    int endPos = Math.max(headEntry.endChar, depEntry.endChar);
                    
                    Position pos = new Position(
                        docId,
                        sentId,
                        beginPos,
                        endPos,
                        headEntry.timestamp
                    );
                    
                    // Add to position list
                    PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
                    posList.add(pos);
                }
            }
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        
        return result;
    }

    @Override
    public void generateIndex() throws SQLException, IOException {
        // Get total count for progress tracking
        try (Statement stmt = sqliteConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM dependencies")) {
            rs.next();
            int totalCount = rs.getInt("total");
            logger.info("Total dependencies to process: {}", totalCount);

            try (ProgressBar pb = new ProgressBar("Indexing dependencies", totalCount)) {
                int offset = 0;
                while (true) {
                    try (Statement batchStmt = sqliteConn.createStatement();
                         ResultSet batchRs = batchStmt.executeQuery(String.format(
                             "SELECT * FROM dependencies ORDER BY document_id, sentence_id LIMIT %d OFFSET %d",
                             BATCH_SIZE, offset))) {

                        if (!batchRs.next()) {
                            break;
                        }

                        do {
                            int documentId = batchRs.getInt("document_id");
                            int sentenceId = batchRs.getInt("sentence_id");
                            String headToken = batchRs.getString("head_token");
                            String dependentToken = batchRs.getString("dependent_token");
                            String relation = batchRs.getString("relation");
                            int beginChar = batchRs.getInt("begin_char");
                            int endChar = batchRs.getInt("end_char");

                            // Create dependency entry
                            DependencyEntry entry = new DependencyEntry(
                                documentId,
                                sentenceId,
                                headToken,
                                dependentToken,
                                relation,
                                beginChar,
                                endChar
                            );

                            // Add to index
                            addToIndex(entry);
                            pb.step();
                        } while (batchRs.next());

                        offset += BATCH_SIZE;
                    }
                }
            }
        }
    }

    private void addToIndex(DependencyEntry entry) throws IOException {
        if (BLACKLISTED_RELATIONS.contains(entry.relation)) {
            return;
        }

        // Create key in format: headToken\0relation\0dependentToken
        String key = String.format("%s\0%s\0%s", entry.headToken, entry.relation, entry.dependentToken);
        
        // Create position information
        PositionList positions = new PositionList();
        positions.add(new Position(entry.documentId, entry.sentenceId, entry.beginChar, entry.endChar, LocalDate.now()));

        // Store in LevelDB
        levelDb.put(bytes(key), positions.serialize());
        logger.debug("Indexed dependency: {} -> {} ({})", entry.headToken, entry.dependentToken, entry.relation);
    }
} 
package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;

/**
 * Generates an index of hypernym-hyponym (category-instance) relationships from dependency annotations.
 * Supports bidirectional lookups through key structure: category${DELIMITER}instance -> PositionList
 * 
 * TODO: This implementation will be refactored as part of the IndexEntry redesign.
 * Current limitations:
 * - Uses IndexEntry.lemma for category (hypernym)
 * - Uses IndexEntry.pos for instance (hyponym)
 * - This overloading of fields will be replaced with proper DependencyEntry class
 */
public class HypernymIndexGenerator extends ParallelIndexGenerator {
    private static final Logger logger = LoggerFactory.getLogger(HypernymIndexGenerator.class);
    
    private static final Set<String> HYPERNYM_RELATIONS = Set.of(
        "nmod:such_as",
        "nmod:as",
        "nmod:including",
        "conj:and",
        "conj:or"
        // Additional patterns will be added in future phases
    );

    /**
     * Creates a new HypernymIndexGenerator
     * 
     * @param levelDbPath Path to the LevelDB directory
     * @param stopwordsPath Path to the stopwords file
     * @param batchSize Size of batches for processing
     * @param sqliteConn Connection to SQLite database
     * @param tableName Name of the annotations table
     * @param threadCount Number of threads to use for parallel processing
     * @throws IOException if there's an error initializing the index
     */
    public HypernymIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, String tableName, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, tableName, threadCount);
    }

    /**
     * TODO: After refactor, this will return List<DependencyEntry> instead of List<IndexEntry>
     * The query logic will remain similar, but the entry creation will be cleaner
     */
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
            
            // Get hypernym relations
            String query = String.format("""
                SELECT d.*, a1.lemma as head_lemma, a2.lemma as dependent_lemma
                FROM dependencies d
                JOIN annotations a1 ON d.document_id = a1.document_id 
                    AND d.sentence_id = a1.sentence_id
                    AND d.head_token = a1.token
                JOIN annotations a2 ON d.document_id = a2.document_id
                    AND d.sentence_id = a2.sentence_id
                    AND d.dependent_token = a2.token
                WHERE d.relation IN (%s)
                ORDER BY d.document_id, d.sentence_id, d.begin_char
                LIMIT %d OFFSET %d
                """, 
                String.join(",", HYPERNYM_RELATIONS.stream().map(r -> "'" + r + "'").toList()),
                batchSize, offset);
            
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    int docId = rs.getInt("document_id");
                    LocalDate timestamp = documentDates.get(docId);
                    if (timestamp == null) {
                        throw new SQLException("Document " + docId + " not found in documents table");
                    }

                    // Create an entry for this hypernym relation
                    String headLemma = rs.getString("head_lemma");
                    String dependentLemma = rs.getString("dependent_lemma");
                    
                    // Skip if either term is a stopword
                    if (isStopword(headLemma) || isStopword(dependentLemma)) {
                        continue;
                    }

                    // TODO: After refactor, this will create a DependencyEntry instead
                    entries.add(new IndexEntry(
                        docId,
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        headLemma,  // category as lemma
                        dependentLemma,  // instance as pos (reusing the field)
                        timestamp
                    ));
                }
            }
        }
        return entries;
    }

    /**
     * TODO: After refactor, this will handle List<DependencyEntry> instead of List<IndexEntry>
     * The position list creation logic will remain similar, but field access will be cleaner
     */
    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (IndexEntry entry : partition) {
            // Create position for this occurrence
            Position position = new Position(
                entry.documentId,
                entry.sentenceId,
                entry.beginChar,
                entry.endChar,
                entry.timestamp
            );

            // TODO: After refactor, will use entry.headToken and entry.dependentToken instead
            String key = createKey(entry.lemma, entry.pos);
            
            // Get or create position list for this hypernym pair
            PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
            posList.add(position);
            
            logger.debug("Added hypernym relation: {} -> {} at position {}", 
                entry.lemma, entry.pos, position);
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }

    /**
     * Creates an index key from a category and instance
     * @param category The hypernym (category)
     * @param instance The hyponym (instance)
     * @return A delimited key in the format category${DELIMITER}instance
     */
    protected String createKey(String category, String instance) {
        return category.toLowerCase() + BaseIndexGenerator.DELIMITER + instance.toLowerCase();
    }
} 
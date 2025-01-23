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
 */
public class HypernymIndexGenerator extends ParallelIndexGenerator<DependencyEntry> {
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


    @Override
    protected List<DependencyEntry> fetchBatch(int offset) throws SQLException {
        List<DependencyEntry> entries = new ArrayList<>();
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

                    // Sanitize text fields
                    String headLemma = sanitizeText(rs.getString("head_lemma"));
                    String dependentLemma = sanitizeText(rs.getString("dependent_lemma"));
                    String relation = sanitizeText(rs.getString("relation"));
                    
                    // Skip if any required field is null or empty after sanitization
                    if (headLemma == null || headLemma.isEmpty() ||
                        dependentLemma == null || dependentLemma.isEmpty() ||
                        relation == null || relation.isEmpty()) {
                        logger.debug("Skipping entry with null/empty fields: head={}, dependent={}, relation={}",
                                   headLemma, dependentLemma, relation);
                        continue;
                    }
                    
                    // Skip if either term is a stopword
                    if (isStopword(headLemma) || isStopword(dependentLemma)) {
                        continue;
                    }

                    entries.add(new DependencyEntry(
                        docId,
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        headLemma,
                        dependentLemma,
                        relation,
                        timestamp
                    ));
                }
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<DependencyEntry> partition) throws IOException {
        ListMultimap<String, PositionList> results = MultimapBuilder.hashKeys().arrayListValues().build();
        
        for (DependencyEntry entry : partition) {
            // Create position for this occurrence
            Position position = new Position(
                entry.getDocumentId(),
                entry.getSentenceId(),
                entry.getBeginChar(),
                entry.getEndChar(),
                entry.getTimestamp()
            );

            // Skip if either term is a stopword
            if (isStopword(entry.getHeadToken()) || isStopword(entry.getDependentToken())) {
                continue;
            }

            // Create key in format: category\0instance
            String key = createKey(entry.getHeadToken(), entry.getDependentToken());
            
            // Create position list for this entry
            PositionList positions = new PositionList();
            positions.add(position);
            
            results.put(key, positions);
            
            logger.debug("Added hypernym relation: {} -> {} at position {}", 
                entry.getHeadToken(), entry.getDependentToken(), position);
        }
        
        return results;
    }

    /**
     * Creates an index key from a category and instance
     * @param category The hypernym (category)
     * @param instance The hyponym (instance)
     * @return A delimited key in the format category${DELIMITER}instance
     */
    protected String createKey(String category, String instance) {
        return category.toLowerCase() + DELIMITER + instance.toLowerCase();
    }
} 
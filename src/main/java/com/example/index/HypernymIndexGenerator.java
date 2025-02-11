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
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;
import com.example.core.Position;
import com.example.core.PositionList;

/**
 * Generates a streaming hypernym index from dependency entries.
 * Each entry maps a hypernym-hyponym (category-instance) pair to its positions in the corpus.
 * Uses streaming processing and external sorting for efficient memory usage.
 */
public final class HypernymIndexGenerator extends IndexGenerator<DependencyEntry> {
    private static final Logger logger = LoggerFactory.getLogger(HypernymIndexGenerator.class);

    private static final Set<String> HYPERNYM_RELATIONS = Set.of(
        "nmod:such_as",
        "nmod:as",
        "nmod:including",
        "nmod:especially",
        "nmod:particularly",
        "conj:and",
        "conj:or"
    );

    public HypernymIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, sqliteConn, progress);
    }

    public HypernymIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress, IndexConfig config) throws IOException {
        super(levelDbPath, stopwordsPath, sqliteConn, progress, config);
    }

    @Override
    protected String getTableName() {
        return "dependencies";
    }

    @Override
    protected String getIndexName() {
        return "hypernym";
    }

    @Override
    protected List<DependencyEntry> fetchBatch(int offset) throws SQLException {
        List<DependencyEntry> batch = new ArrayList<>();
        
        // Build the IN clause from HYPERNYM_RELATIONS
        String inClause = HYPERNYM_RELATIONS.stream()
            .map(r -> "'" + r + "'")
            .collect(java.util.stream.Collectors.joining(", "));
            
        String query = String.format(
            "SELECT d.document_id, d.sentence_id, " +
            "a1.lemma as head_lemma, a2.lemma as dependent_lemma, " +
            "d.relation, d.begin_char, d.end_char, doc.timestamp " +
            "FROM dependencies d " +
            "JOIN documents doc ON d.document_id = doc.document_id " +
            "JOIN annotations a1 ON d.document_id = a1.document_id " +
            "  AND d.sentence_id = a1.sentence_id " +
            "  AND d.head_token = a1.token " +
            "JOIN annotations a2 ON d.document_id = a2.document_id " +
            "  AND d.sentence_id = a2.sentence_id " +
            "  AND d.dependent_token = a2.token " +
            "WHERE d.relation IN (%s) " +
            "ORDER BY d.document_id, d.sentence_id, d.begin_char LIMIT ? OFFSET ?",
            inClause);
        
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
            stmt.setInt(1, config.getBatchSize());
            stmt.setInt(2, offset);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
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

                    batch.add(new DependencyEntry(
                        rs.getInt("document_id"),
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        headLemma,
                        dependentLemma,
                        relation,
                        LocalDate.parse(rs.getString("timestamp").substring(0, 10))
                    ));
                }
            }
        }
        return batch;
    }

    @Override
    protected ListMultimap<String, PositionList> processBatch(List<DependencyEntry> batch) throws IOException {
        ListMultimap<String, PositionList> index = ArrayListMultimap.create();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (DependencyEntry entry : batch) {
            // Create position for this occurrence
            Position position = new Position(
                entry.getDocumentId(),
                entry.getSentenceId(),
                entry.getBeginChar(),
                entry.getEndChar(),
                entry.getTimestamp()
            );

            // Create key in format: category\0instance
            String key = createKey(entry.getHeadToken(), entry.getDependentToken());
            
            // Get or create position list for this hypernym pair
            PositionList posList = positionLists.computeIfAbsent(key, k -> new PositionList());
            posList.add(position);
            
            logger.debug("Added hypernym relation: {} -> {} at position {}", 
                entry.getHeadToken(), entry.getDependentToken(), position);
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
        return category.toLowerCase() + DELIMITER + instance.toLowerCase();
    }

    /**
     * Sanitizes text by removing special characters and normalizing whitespace.
     * @param text The text to sanitize
     * @return The sanitized text, or null if the input is null or empty
     */
    private String sanitizeText(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        
        // Special handling for relation names to preserve colons
        if (text.startsWith("nmod:")) {
            return text.trim().replaceAll("\\s+", " ");
        }
        
        return text.trim()
                  .replaceAll("\\s+", " ")
                  .replaceAll("[^\\p{L}\\p{N}\\s:-]", "");
    }
} 
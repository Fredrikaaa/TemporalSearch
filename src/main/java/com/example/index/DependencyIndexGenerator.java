package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * Generates a dependency index from dependency relation entries.
 * Each entry maps a head token, relation type, and dependent token to their positions in the corpus.
 */
public final class DependencyIndexGenerator extends BaseIndexGenerator<DependencyEntry> {
    private static final Logger logger = LoggerFactory.getLogger(DependencyIndexGenerator.class);

    private static final Set<String> BLACKLISTED_RELATIONS = Set.of(
        "punct", "det", "case", "cc"
    );

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies");
    }

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies", threadCount);
    }

    @Override
    protected List<DependencyEntry> fetchBatch(int offset) throws SQLException {
        List<DependencyEntry> entries = new ArrayList<>();
        String sql = """
            SELECT d.document_id, d.sentence_id, d.head_token, d.dependent_token,
                   d.relation, d.begin_char, d.end_char, doc.timestamp
            FROM dependencies d
            JOIN documents doc ON d.document_id = doc.document_id
            ORDER BY d.document_id, d.sentence_id
            LIMIT ? OFFSET ?
        """;

        try (PreparedStatement stmt = sqliteConn.prepareStatement(sql)) {
            stmt.setInt(1, batchSize);
            stmt.setInt(2, offset);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // Sanitize text fields
                    String headToken = sanitizeText(rs.getString("head_token"));
                    String dependentToken = sanitizeText(rs.getString("dependent_token"));
                    String relation = sanitizeText(rs.getString("relation"));

                    // Skip entries where any required field is null or empty after sanitization
                    if (headToken == null || headToken.isEmpty() ||
                        dependentToken == null || dependentToken.isEmpty() ||
                        relation == null || relation.isEmpty()) {
                        logger.debug("Skipping entry with null/empty fields: head={}, dependent={}, relation={}",
                                   headToken, dependentToken, relation);
                        continue;
                    }

                    entries.add(new DependencyEntry(
                        rs.getInt("document_id"),
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        headToken,
                        dependentToken,
                        relation,
                        LocalDate.parse(rs.getString("timestamp").substring(0, 10))
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
            // Skip stopwords and blacklisted relations
            if (isStopword(entry.getHeadToken()) || 
                isStopword(entry.getDependentToken()) ||
                BLACKLISTED_RELATIONS.contains(entry.getRelation())) {
                continue;
            }

            // Create key in format: headToken\0relation\0dependentToken
            String key = generateKey(entry);

            // Create position list for this entry
            PositionList positions = new PositionList();
            positions.add(new Position(
                entry.getDocumentId(),
                entry.getSentenceId(),
                entry.getBeginChar(),
                entry.getEndChar(),
                entry.getTimestamp()
            ));

            results.put(key, positions);
        }

        return results;
    }

    // Helper method for key generation
    protected String generateKey(DependencyEntry entry) {
        return String.format("%s%s%s%s%s", 
            entry.getHeadToken().trim().toLowerCase(),
            DELIMITER,
            entry.getRelation().trim().toLowerCase(),
            DELIMITER,
            entry.getDependentToken().trim().toLowerCase());
    }
} 
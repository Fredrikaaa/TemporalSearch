package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Phase 1 implementation for indexing syntactic dependencies.
 *
 * Creates dependency indexes containing head token, relation, and dependent
 * token
 * with position information. Filters out noisy relations like punct, cc, det,
 * case.
 */
public class DependencyIndexGenerator extends BaseIndexGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DependencyIndexGenerator.class);

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies");
    }

    public DependencyIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, int threadCount) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "dependencies", threadCount);
    }

    @Override
    protected List<IndexEntry> fetchBatch(long offset) throws SQLException {
        List<IndexEntry> entries = new ArrayList<>();

        String query = String.format("""
                    SELECT dep.document_id, dep.sentence_id, dep.begin_char, dep.end_char,
                           dep.head_token, dep.dependent_token, dep.relation, d.timestamp
                    FROM dependencies dep
                    JOIN documents d ON dep.document_id = d.document_id
                    WHERE dep.relation NOT IN ('punct', 'cc', 'det', 'case')
                    ORDER BY dep.document_id, dep.sentence_id, dep.begin_char
                    LIMIT %d OFFSET %d
                """, batchSize, offset);

        try (Statement stmt = sqliteConn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String timestamp = rs.getString("timestamp");
                LocalDate date = ZonedDateTime.parse(timestamp).toLocalDate();

                DependencyIndexEntry entry = new DependencyIndexEntry(
                        rs.getInt("document_id"),
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        null,
                        null,
                        date,
                        rs.getString("head_token"),
                        rs.getString("dependent_token"),
                        rs.getString("relation"));

                entries.add(entry);
            }
        }

        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<IndexEntry> partition) {
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();

        for (IndexEntry entry : partition) {
            if (!(entry instanceof DependencyIndexEntry)) {
                logger.warn("Skipping unexpected entry type: {}", entry.getClass());
                continue;
            }

            DependencyIndexEntry dep = (DependencyIndexEntry) entry;

            // Create composite key with null byte delimiters
            String key = dep.headToken + "\0" + dep.relation + "\0" + dep.dependentToken;

            Position position = new Position(
                    dep.documentId,
                    dep.sentenceId,
                    dep.beginChar,
                    dep.endChar,
                    dep.timestamp);

            // Add position to index
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

    private static class DependencyIndexEntry extends IndexEntry {
        final String headToken;
        final String dependentToken;
        final String relation;

        DependencyIndexEntry(
                int documentId,
                int sentenceId,
                int beginChar,
                int endChar,
                String lemma,
                String pos,
                LocalDate timestamp,
                String headToken,
                String dependentToken,
                String relation) {
            super(documentId, sentenceId, beginChar, endChar, lemma, pos, timestamp);
            this.headToken = headToken;
            this.dependentToken = dependentToken;
            this.relation = relation;
        }
    }
}

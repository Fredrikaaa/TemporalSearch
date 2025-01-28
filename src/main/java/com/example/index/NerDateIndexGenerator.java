package com.example.index;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import com.example.logging.ProgressTracker;

/**
 * Generates indexes for date entities from annotated text.
 * Extracts dates from the normalized_ner column where ner type is "DATE",
 * normalizes them to YYYYMMDD format, and stores their positions.
 * 
 * @deprecated Use {@link StreamingNerDateIndexGenerator} instead. This implementation will be removed in a future release.
 */
@Deprecated(since = "2.0", forRemoval = true)
public class NerDateIndexGenerator extends BaseIndexGenerator<AnnotationEntry> {
    private static final Logger logger = LoggerFactory.getLogger(NerDateIndexGenerator.class);
    private static final DateTimeFormatter INPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter KEY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public NerDateIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations");
    }

    public NerDateIndexGenerator(String levelDbPath, String stopwordsPath,
            int batchSize, Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, batchSize, sqliteConn, "annotations", 
              Runtime.getRuntime().availableProcessors(), progress);
    }

    @Override
    protected List<AnnotationEntry> fetchBatch(int offset) throws SQLException {
        List<AnnotationEntry> entries = new ArrayList<>();
        try (Statement stmt = sqliteConn.createStatement()) {
            // First get document timestamps
            Map<Integer, LocalDate> documentDates = new HashMap<>();
            try (ResultSet rs = stmt.executeQuery("SELECT document_id, timestamp FROM documents")) {
                while (rs.next()) {
                    documentDates.put(
                        rs.getInt("document_id"),
                        LocalDate.parse(rs.getString("timestamp").substring(0, 10))
                    );
                }
            }
            
            // Fetch date entities with their normalized values
            ResultSet rs = stmt.executeQuery(String.format(
                "SELECT document_id, sentence_id, begin_char, end_char, normalized_ner " +
                "FROM annotations WHERE ner = 'DATE' AND normalized_ner IS NOT NULL " +
                "AND normalized_ner LIKE '____-__-__' " + // Match YYYY-MM-DD pattern
                "AND substr(normalized_ner, 1, 4) BETWEEN '0000' AND '9999' " + // Year validation
                "AND substr(normalized_ner, 6, 2) BETWEEN '01' AND '12' " + // Month validation
                "AND substr(normalized_ner, 9, 2) BETWEEN '01' AND '31' " + // Day validation
                "ORDER BY document_id, sentence_id, begin_char " +
                "LIMIT %d OFFSET %d",
                batchSize, offset));
            
            while (rs.next()) {
                int docId = rs.getInt("document_id");
                LocalDate timestamp = documentDates.get(docId);
                if (timestamp == null) {
                    logger.warn("Document {} not found in documents table", docId);
                    continue;
                }
                
                entries.add(new AnnotationEntry(
                    docId,
                    rs.getInt("sentence_id"),
                    rs.getInt("begin_char"),
                    rs.getInt("end_char"),
                    rs.getString("normalized_ner"), // Store normalized date in lemma field
                    "DATE", // Store NER type in pos field
                    timestamp
                ));
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processPartition(List<AnnotationEntry> partition) throws IOException {
        if (partition == null) {
            throw new IOException("Partition cannot be null");
        }
        
        ListMultimap<String, PositionList> index = MultimapBuilder.hashKeys().arrayListValues().build();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (AnnotationEntry entry : partition) {
            String normalizedDate = normalizeDate(entry.getLemma());
            if (normalizedDate == null) {
                logger.debug("Skipping invalid date format: {}", entry.getLemma());
                continue;
            }

            Position position = new Position(
                entry.getDocumentId(), 
                entry.getSentenceId(), 
                entry.getBeginChar(), 
                entry.getEndChar(), 
                entry.getTimestamp()
            );

            // Get or create position list for this date
            PositionList posList = positionLists.computeIfAbsent(normalizedDate, k -> new PositionList());
            posList.add(position);
        }
        
        // Add all position lists to result
        for (Map.Entry<String, PositionList> entry : positionLists.entrySet()) {
            index.put(entry.getKey(), entry.getValue());
        }
        
        return index;
    }

    /**
     * Normalizes a date string from YYYY-MM-DD format to YYYYMMDD format.
     * Returns null if the input is not a valid date or not in the expected format.
     */
    private String normalizeDate(String date) {
        if (date == null || !date.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return null;
        }
        try {
            LocalDate parsed = LocalDate.parse(date, INPUT_FORMAT);
            return parsed.format(KEY_FORMAT);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
} 
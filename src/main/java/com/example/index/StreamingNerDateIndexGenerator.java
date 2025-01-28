package com.example.index;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.logging.ProgressTracker;

/**
 * Generates a streaming index for date entities from annotated text.
 * Extracts dates from the normalized_ner column where ner type is "DATE",
 * normalizes them to YYYYMMDD format, and stores their positions.
 * Uses streaming processing and external sorting for efficient memory usage.
 */
public final class StreamingNerDateIndexGenerator extends StreamingIndexGenerator<AnnotationEntry> {
    private static final Logger logger = LoggerFactory.getLogger(StreamingNerDateIndexGenerator.class);
    private static final int BATCH_SIZE = 1000;
    private static final DateTimeFormatter INPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter KEY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public StreamingNerDateIndexGenerator(String levelDbPath, String stopwordsPath,
            Connection sqliteConn, ProgressTracker progress) throws IOException {
        super(levelDbPath, stopwordsPath, sqliteConn, progress);
    }

    @Override
    protected List<AnnotationEntry> fetchBatch(int offset) throws SQLException {
        List<AnnotationEntry> entries = new ArrayList<>();
        String sql = """
            SELECT a.document_id, a.sentence_id, a.begin_char, a.end_char, 
                   a.normalized_ner, doc.timestamp
            FROM annotations a
            JOIN documents doc ON a.document_id = doc.document_id
            WHERE a.ner = 'DATE' 
              AND a.normalized_ner IS NOT NULL
              AND a.normalized_ner LIKE '____-__-__'
              AND substr(a.normalized_ner, 1, 4) BETWEEN '0000' AND '9999'
              AND substr(a.normalized_ner, 6, 2) BETWEEN '01' AND '12'
              AND substr(a.normalized_ner, 9, 2) BETWEEN '01' AND '31'
            ORDER BY a.document_id, a.sentence_id, a.begin_char
            LIMIT ? OFFSET ?
        """;

        try (PreparedStatement stmt = sqliteConn.prepareStatement(sql)) {
            stmt.setInt(1, BATCH_SIZE);
            stmt.setInt(2, offset);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String normalizedDate = rs.getString("normalized_ner");
                    if (normalizedDate == null || !normalizedDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        logger.debug("Skipping invalid date format: {}", normalizedDate);
                        continue;
                    }

                    entries.add(new AnnotationEntry(
                        rs.getInt("document_id"),
                        rs.getInt("sentence_id"),
                        rs.getInt("begin_char"),
                        rs.getInt("end_char"),
                        normalizedDate, // Store normalized date in lemma field
                        "DATE", // Store NER type in pos field
                        LocalDate.parse(rs.getString("timestamp").substring(0, 10))
                    ));
                }
            }
        }
        return entries;
    }

    @Override
    protected ListMultimap<String, PositionList> processBatch(List<AnnotationEntry> batch) throws IOException {
        ListMultimap<String, PositionList> index = ArrayListMultimap.create();
        Map<String, PositionList> positionLists = new HashMap<>();
        
        for (AnnotationEntry entry : batch) {
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